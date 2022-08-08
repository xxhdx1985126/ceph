// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

/**
 * RootBlock
 *
 * Holds the physical addresses of all metadata roots.
 * In-memory values may be
 * - absolute: reference to block which predates the current transaction
 * - record_relative: reference to block updated in this transaction
 *   if !pending()
 *
 * Journal replay only considers deltas and must always discover the most
 * recent value for the RootBlock.  Because the contents of root_t above are
 * very small, it's simplest to stash the entire root_t value into the delta
 * and never actually write the RootBlock to a physical location (safe since
 * nothing references the location of the RootBlock).
 *
 * As a result, Cache treats the root differently in a few ways including:
 * - state will only ever be DIRTY or MUTATION_PENDING
 * - RootBlock's never show up in the transaction fresh or dirty lists --
 *   there's a special Transaction::root member for when the root needs to
 *   be mutated.
 *
 * TODO: Journal trimming will need to be aware of the most recent RootBlock
 * delta location, or, even easier, just always write one out with the
 * mutation which changes the journal trim bound.
 */
struct RootBlock : CachedExtent {
  constexpr static seastore_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<RootBlock>;

  root_t root;

  RootBlock() : CachedExtent(0) {}

  RootBlock(const RootBlock &rhs) = default;

  CachedExtentRef duplicate_for_write(Transaction &) final {
    return CachedExtentRef(new RootBlock(*this));
  }

  static constexpr extent_types_t TYPE = extent_types_t::ROOT;
  extent_types_t get_type() const final {
    return extent_types_t::ROOT;
  }

  /// dumps root as delta
  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    ceph::buffer::ptr bptr(sizeof(root_t));
    *reinterpret_cast<root_t*>(bptr.c_str()) = root;
    bl.append(bptr);
    return bl;
  }

  /// overwrites root
  void apply_delta_and_adjust_crc(paddr_t base, const ceph::bufferlist &_bl) final {
    assert(_bl.length() == sizeof(root_t));
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    root = *reinterpret_cast<const root_t*>(bl.front().c_str());
    root.adjust_addrs_from_base(base);
  }

  /// Patches relative addrs in memory based on record commit addr
  void on_delta_commit(paddr_t record_block_offset) final {
    root.adjust_addrs_from_base(record_block_offset);
  }

  void on_replace_extent(Transaction&, CachedExtent&) final {}

  complete_load_ertr::future<> complete_load() final {
    ceph_abort_msg("Root is only written via deltas");
  }

  void on_initial_write() final {
    ceph_abort_msg("Root is only written via deltas");
  }

  root_t &get_root() { return root; }

};
using RootBlockRef = RootBlock::Ref;

}
