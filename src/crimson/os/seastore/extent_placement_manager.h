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

class ExtentAllocWriter {
public:
  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >;
  using roll_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using alloc_extent_ertr = roll_segment_ertr;

  ExtentAllocWriter(SegmentProvider& sp, SegmentManager& sm)
    : segment_provider(sp), segment_manager(sm) {}
  alloc_extent_ertr::future<alloc_t> alloc(segment_off_t length);
  // may return the id of the segment that will be closed
  write_ertr::future<SegmentRef> write(CachedExtent* extent);
private:
  bool _needs_roll(segment_off_t length) const;
  roll_segment_ertr::future<> roll_segment();

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  SegmentRef current_segment;
  std::vector<SegmentRef> open_segments;
  segment_off_t allocated_to = 0;
};

struct SegmentAllocator {
public:
  using alloc_extent_ertr = ExtentAllocWriter::alloc_extent_ertr;
  SegmentAllocator(SegmentProvider& sp, SegmentManager& sm, Cache& cache)
    : segment_provider(sp), segment_manager(sm), cache(cache) {}
  template <typename T>
  alloc_extent_ertr::future<TCachedExtentRef<T>> alloc(
    lifetime_level ltl,
    segment_off_t length)
  {
    auto iter = allocators.find(ltl);
    if (iter == allocators.end()) {
      iter = allocators.try_emplace(ltl, segment_provider, segment_manager).first;
    }
    auto& allocator = iter->second;

    return allocator.alloc(length).safe_then(
      [this, &allocator, length](auto alloc_addr) {
      auto nextent = cache.alloc_new_extent<T>(length);
      nextent->set_paddr(std::move(alloc_addr.addr));
      nextent->segment_allocated(alloc_addr.segment);
      nextent->extent_writer = &allocator;
      return nextent;
    });
  }
private:
  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::map<lifetime_level, ExtentAllocWriter> allocators;
  Cache& cache;
};

class ExtentPlacementManager {
public:
  using alloc_extent_ertr = ExtentAllocWriter::alloc_extent_ertr;
  ExtentPlacementManager(SegmentProvider& segment_provider, Cache& cache)
    : segment_provider(segment_provider), cache(cache) {}
  // choose, based on the infered_lifetime, a segment
  // to allocate a block of the specified size
  template <typename T>
  alloc_extent_ertr::future<TCachedExtentRef<T>> alloc_new_extent(
    segment_off_t size,
    hint_t hint) {
    auto h = calc_heat(hint);
    auto iter = segment_allocators.find(h);
    auto& allocator = iter->second;
    auto l = predict_lifetime(hint);

    return allocator.alloc<T>(l, size);
  }

  alloc_extent_ertr::future<CachedExtentRef> alloc_new_extent_by_type(
    extent_types_t type,
    segment_off_t length);

  void add_segment_manager(heat_level hl, SegmentManager& smr) {
    segment_allocators.try_emplace(hl, segment_provider, smr, cache);
  }

protected:
  virtual heat_level calc_heat(hint_t hint) {
    return heat_level::DEFAULT;
  }
  virtual lifetime_level predict_lifetime(hint_t hit) {
    return lifetime_level::DEFAULT;
  }
private:
  std::map<heat_level, SegmentAllocator> segment_allocators;
  SegmentProvider& segment_provider;
  Cache& cache;
};

using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
