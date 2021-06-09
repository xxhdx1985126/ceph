// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/object_data_handler.h"

namespace crimson::os::seastore {

ExtentAllocWriter::write_ertr::future<SegmentRef>
ExtentAllocWriter::write(CachedExtent* extent) {
  bufferlist bl;
  extent->prepare_write();
  bl.append(extent->get_bptr());
  auto segment = extent->get_allocated_segment();
  assert(extent->extent_writer);
  assert(segment);
  auto old_segment_iter = open_segments.begin();
  SegmentRef closed_segment;
  if (segment.get() != old_segment_iter->get()) {
    // segment writes are ordered, so when an opened segment isn't the current
    // one being written to, it is full and should be closed.
    closed_segment = std::move(*old_segment_iter);
    open_segments.erase(old_segment_iter);
  }
  write_to = extent->get_paddr();
  assert(write_to.segment = segment->get_segment_id());
  assert(write_to.offset >= allocated_to);
  return segment->write(write_to.offset, bl).safe_then(
    [this, extent, segment, closed_segment=std::move(closed_segment)] {
    extent->extent_writer = nullptr;
    extent->clear_allocated_segment();
    committed_to = extent->get_paddr();
    assert(committed_to >= write_to);
    return write_ertr::make_ready_future<SegmentRef>(closed_segment);
  });
}

ExtentAllocWriter::alloc_extent_ertr::future<alloc_t>
ExtentAllocWriter::alloc(segment_off_t length) {
  auto roll_fut = roll_segment_ertr::make_ready_future<>();
  if (_needs_roll(length)) {
    roll_fut = roll_segment();
  }
  return roll_fut.safe_then([this, length] {
    paddr_t addr{current_segment->get_segment_id(), allocated_to};
    alloc_t alloc_addr{addr, current_segment};
    allocated_to += length;
    return alloc_addr;
  });
}

bool ExtentAllocWriter::_needs_roll(segment_off_t length) const {
  return !current_segment || (allocated_to + length >
          current_segment->get_write_capacity());
}

ExtentAllocWriter::roll_segment_ertr::future<>
ExtentAllocWriter::roll_segment() {
  return segment_provider.get_segment().safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto segref) {
    allocated_to = 0;
    current_segment = segref;
    open_segments.emplace_back(segref);
  }).handle_error(
    roll_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
  );
}

ExtentPlacementManager::alloc_extent_ertr::future<CachedExtentRef>
ExtentPlacementManager::alloc_new_extent_by_type(
  extent_types_t type,
  segment_off_t length)
{
  switch (type) {
  case extent_types_t::ROOT:
    assert(0 == "ROOT is never directly alloc'd");
    return alloc_extent_ertr::make_ready_future<CachedExtentRef>(CachedExtentRef());
  case extent_types_t::LADDR_INTERNAL:
    return alloc_new_extent<lba_manager::btree::LBAInternalNode>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::LADDR_LEAF:
    return alloc_new_extent<lba_manager::btree::LBALeafNode>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::ONODE_BLOCK_STAGED:
    return alloc_new_extent<onode::SeastoreNodeExtent>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::OMAP_INNER:
    return alloc_new_extent<omap_manager::OMapInnerNode>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::OMAP_LEAF:
    return alloc_new_extent<omap_manager::OMapLeafNode>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::COLL_BLOCK:
    return alloc_new_extent<collection_manager::CollectionNode>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::OBJECT_DATA_BLOCK:
    return alloc_new_extent<ObjectDataBlock>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::RETIRED_PLACEHOLDER:
    ceph_assert(0 == "impossible");
    return alloc_extent_ertr::make_ready_future<CachedExtentRef>(CachedExtentRef());
  case extent_types_t::TEST_BLOCK:
    return alloc_new_extent<TestBlock>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return alloc_new_extent<TestBlockPhysical>(
      length, {}).safe_then([](auto extent) {
        return alloc_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      });
  case extent_types_t::NONE: {
    ceph_assert(0 == "NONE is an invalid extent type");
    return alloc_extent_ertr::make_ready_future<CachedExtentRef>(CachedExtentRef());
  }
  default:
    ceph_assert(0 == "impossible");
    return alloc_extent_ertr::make_ready_future<CachedExtentRef>(CachedExtentRef());
  }
  return alloc_extent_ertr::make_ready_future<CachedExtentRef>(CachedExtentRef());
}

}
