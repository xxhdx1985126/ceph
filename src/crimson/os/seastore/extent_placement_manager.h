// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

enum class heat_level {
  //XXX: this is just a place holder for now!
  DEFAULT = 0
};

enum class lifetime_level {
  //XXX: this is just a place holder for now!
  DEFAULT = 0
};

//XXX: this is just a place holder for now!
struct hint_t {};

struct alloc_t {
  paddr_t addr;
  SegmentRef segment;
};

#define REWRITE_SEGMENT_HEADER_MAGIC "extent_placement_manager segment header v1"

namespace extent_placement_manager {
  struct segment_header_t {
    std::string magic;
    DENC(segment_header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.magic, p);
      DENC_FINISH(p);
    }
  };
}

class ExtentRewriter {
public:
  using write_iertr = trans_iertr<crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >>;

  virtual write_iertr::future<> write(std::list<LogicalCachedExtentRef>& extent) = 0;
};

class SegmentedRewriter : public ExtentRewriter,
                          public boost::intrusive_ref_counter<
  SegmentedRewriter, boost::thread_unsafe_counter>{
public:
  using roll_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_ertr = roll_segment_ertr;
  using init_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  SegmentedRewriter(SegmentProvider& sp, SegmentManager& sm)
    : segment_provider(sp), segment_manager(sm) {}
  write_iertr::future<> write(std::list<LogicalCachedExtentRef>& extent) final;

private:
  bool _needs_roll(segment_off_t length) const;
  roll_segment_ertr::future<> roll_segment();
  init_segment_ertr::future<> init_segment(Segment& segment);

  using extents_to_write_t = std::vector<LogicalCachedExtentRef>;
  void add_extent_to_write(bufferlist& bl,
                           extents_to_write_t& extents_to_write,
                           LogicalCachedExtentRef& extent);

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  SegmentRef current_segment;
  std::vector<SegmentRef> open_segments;
  segment_off_t allocated_to = 0;
};

using SegmentedRewriterRef = std::unique_ptr<SegmentedRewriter>;

class ExtentAllocator {
public:
  using scan_device_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual CachedExtentRef alloc_ool_extent(
    extent_types_t type,
    hint_t hint,
    segment_off_t length) = 0;

  virtual scan_device_ertr::future<> scan_device() = 0;
};

using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

class SegmentedAllocator : public ExtentAllocator {
public:
  using alloc_extent_ertr = SegmentedRewriter::alloc_extent_ertr;
  SegmentedAllocator(SegmentProvider& sp, SegmentManager& sm, Cache& cache)
    : segment_provider(sp), segment_manager(sm), cache(cache) {}

  CachedExtentRef alloc_ool_extent(
    extent_types_t type,
    hint_t hint,
    segment_off_t length) final;


  template <typename T>
  TCachedExtentRef<T> alloc_ool_extent(
    hint_t hint,
    segment_off_t length)
  {
    auto ltl = predict_lifetime(hint);
    auto iter = writers.find(ltl);
    if (iter == writers.end()) {
      iter = writers.emplace(
        ltl,
        std::make_unique<SegmentedRewriter>(
          segment_provider,
          segment_manager)).first;
    }
    auto& writer = iter->second;

    auto nextent = cache.alloc_new_extent<T>(length);
    nextent->set_paddr({UNKNOWN_ABSOLUTE_ADDR, fake_paddr_off});
    fake_paddr_off += length;
    nextent->extent_writer = writer.get();
    return nextent;
  }

  ExtentAllocator::scan_device_ertr::future<> scan_device() final;
private:
  virtual lifetime_level predict_lifetime(hint_t hit) {
    return lifetime_level::DEFAULT;
  }

  int64_t fake_paddr_off = 0;

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::map<lifetime_level, SegmentedRewriterRef> writers;
  Cache& cache;
};

class ExtentPlacementManager {
public:
  using alloc_extent_ertr = SegmentedRewriter::alloc_extent_ertr;
  using scan_devices_ertr = SegmentedAllocator::scan_device_ertr;
  ExtentPlacementManager(Cache& cache) : cache(cache) {}

  CachedExtentRef alloc_new_extent_by_type(
    extent_types_t type,
    hint_t hint,
    segment_off_t length) {
    auto h = calc_heat(hint);
    auto iter = extent_allocators.find(h);
    auto& allocator = iter->second;

    return allocator->alloc_ool_extent(type, hint, length);
  }

  void add_allocator(heat_level hl, ExtentAllocatorRef&& allocator) {
    auto [it, inserted] = extent_allocators.emplace(hl, std::move(allocator));
    assert(inserted);
  }

  scan_devices_ertr::future<> scan_devices() {
    return crimson::do_for_each(
      extent_allocators,
      [this](auto& entry) {
      auto& extent_allocator = entry.second;
      return extent_allocator->scan_device();
    });
  }

protected:
  virtual heat_level calc_heat(hint_t hint) {
    return heat_level::DEFAULT;
  }
private:
  std::map<heat_level, ExtentAllocatorRef> extent_allocators;
  Cache& cache;
};

using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_placement_manager::segment_header_t)
