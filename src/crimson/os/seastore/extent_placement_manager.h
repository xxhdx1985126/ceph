// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once
#include "crimson/os/seastore/cache.h"

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

  void get_live_segments(std::map<segment_id_t, segment_off_t>& segments) {
    segments.emplace(committed_to.segment, committed_to.offset);
  }

  void set_current_segment(SegmentRef segment, segment_off_t alloc_to) {
    current_segment = segment;
    open_segments.emplace_back(segment);
    allocated_to = alloc_to;
  }

private:
  bool _needs_roll(segment_off_t length) const;
  roll_segment_ertr::future<> roll_segment();

  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  SegmentRef current_segment;
  std::vector<SegmentRef> open_segments;
  segment_off_t allocated_to = 0;
  paddr_t write_to = {NULL_SEG_ID, 0};
  paddr_t committed_to = {NULL_SEG_ID, 0};

};

using ExtentAllocWriterRef = seastar::shared_ptr<ExtentAllocWriter>;

class SegmentAllocator {
public:
  using alloc_extent_ertr = ExtentAllocWriter::alloc_extent_ertr;
  SegmentAllocator(SegmentProvider& sp, SegmentManager& sm, Cache& cache)
    : segment_provider(sp), segment_manager(sm), cache(cache) {}
  alloc_extent_ertr::future<std::tuple<alloc_t, ExtentAllocWriterRef>> alloc(
    lifetime_level ltl,
    segment_off_t length)
  {
    auto iter = allocators.find(ltl);
    if (iter == allocators.end()) {
      iter = allocators.emplace(
          ltl,
          seastar::make_shared<ExtentAllocWriter>(
            segment_provider, segment_manager)).first;
    }
    auto& allocator = iter->second;

    return allocator->alloc(length).safe_then([allocator](auto alloc_addr) {
      return alloc_extent_ertr::make_ready_future<
        std::tuple<alloc_t, ExtentAllocWriterRef>>(
            std::make_tuple(alloc_addr, allocator));
    });
  }

  using add_allocator_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  add_allocator_ertr::future<> add_allocator(
    lifetime_level ltl,
    segment_id_t segment_id,
    segment_off_t alloc_to)
  {
    return segment_manager.open(segment_id).safe_then(
      [this, ltl, alloc_to](auto segment) {
      auto [iter, inserted] = allocators.emplace(
        ltl,
        seastar::make_shared<ExtentAllocWriter>(
          segment_provider, segment_manager));
      assert(inserted);
      auto& allocator = iter->second;
      allocator->set_current_segment(segment, alloc_to);
      return add_allocator_ertr::now();
    });
  }
  void get_live_segments(std::map<segment_id_t, segment_off_t>& segments) {
    for (auto& [ll, writer] : allocators) {
      writer->get_live_segments(segments);
    }
  }
private:
  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::map<lifetime_level, ExtentAllocWriterRef> allocators;
  Cache& cache;
};

using SegmentAllocatorRef = seastar::shared_ptr<SegmentAllocator>;

class ExtentPlacementManager {
public:
  using segment_manager_id_t = uint16_t;
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

    return allocator->alloc(l, size).safe_then(
      [this, size](auto tpl) {
      auto alloc_addr = std::get<0>(tpl);
      auto allocator = std::get<1>(tpl);
      auto nextent = cache.alloc_new_extent<T>(size);
      nextent->set_paddr(std::move(alloc_addr.addr));
      nextent->segment_allocated(alloc_addr.segment);
      nextent->extent_writer = allocator;
      return nextent;
    });
  }

  alloc_extent_ertr::future<CachedExtentRef> alloc_new_extent_by_type(
    extent_types_t type,
    segment_off_t length);

  void add_segment_manager(heat_level hl, SegmentManager& smr) {
    auto segment_allocator = seastar::make_shared<SegmentAllocator>(
        segment_provider, smr, cache);
    segment_allocators.emplace(hl, segment_allocator);
    segment_allocator_initializers.emplace(
        smr.get_segment_manager_id(), segment_allocator);
  }

  using new_placement_ertr = SegmentAllocator::add_allocator_ertr;
  new_placement_ertr::future<> add_new_placement(
    heat_level hl,
    lifetime_level ltl,
    segment_id_t segment,
    segment_off_t alloc_to)
  {
    auto iter = segment_allocators.find(hl);
    assert(iter != segment_allocators.end());
    auto& segment_allocator = iter->second;
    return segment_allocator->add_allocator(ltl, segment, alloc_to);
  }

  std::map<segment_id_t, segment_off_t> get_live_segments() {
    std::map<segment_id_t, segment_off_t> segments;
    for (auto& [hl, segment_allocator] : segment_allocators) {
      segment_allocator->get_live_segments(segments);
    }
    return segments;
  }

protected:
  virtual heat_level calc_heat(hint_t hint) {
    return heat_level::DEFAULT;
  }
  virtual lifetime_level predict_lifetime(hint_t hit) {
    return lifetime_level::DEFAULT;
  }
private:
  std::map<heat_level, SegmentAllocatorRef> segment_allocators;
  std::map<segment_manager_id_t, SegmentAllocatorRef>
  segment_allocator_initializers;
  SegmentProvider& segment_provider;
  Cache& cache;
};

using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
