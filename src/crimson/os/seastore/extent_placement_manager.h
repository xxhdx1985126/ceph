// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

namespace crimson::os::seastore {

struct hint_t {
};

class ExtentAllocator {
private:
  SegmentRef segment;
  segment_off_t written_to;
  segment_off_t committed_to;
};

class ExtentPlacementManager {
public:
  // choose, based on the infered_lifetime, a segment
  // to allocate a block of the specified size
  alloc_extent_ertr::future<CachedExtentRef> alloc_extent(
    size_t size,
    hint_t hint);

  // write the extent to the corresponding segment
  write_ertr::future<paddr_t> write(CachedExtentRef extent);

private:
  std::map<LifetimeLevel, std::tuple<SegmentManagerRef, SegmentState>> segments;
  std::map<segment_manager_id_t, SegmentManagerRef> segment_managers;
  CacheRef cache;
};

}
