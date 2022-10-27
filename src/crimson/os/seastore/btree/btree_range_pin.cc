// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

template <typename key_t, typename val_t>
void BtreeNodePin<key_t, val_t>::link_extent(LogicalCachedExtent *ref) {
  assert(ref->is_valid());
  // it's only when reading logical extents from disk that we need to
  // link them to lba leaves
  if (!ref->is_pending() && !ref->is_exist_clean()) {
    assert(parent);
    assert(pos != std::numeric_limits<uint16_t>::max());
    if (parent->is_initial_pending()) {
      auto &p = ((FixedKVNode<key_t>&)*parent).get_stable_for_key(
	pin.range.begin);
      p.link_child(ref, pos);
    } else if (parent->is_mutation_pending()) {
      auto &p = (FixedKVNode<key_t>&)*parent->get_prior_instance();
      p.link_child(ref, pos);
    } else {
      assert(!parent->is_pending() && parent->is_valid());
      auto &p = (FixedKVNode<key_t>&)*parent;
      p.link_child(ref, pos);
    }
  }
  pos = std::numeric_limits<uint16_t>::max();
  pin.set_extent(ref);
}

template <typename key_t, typename val_t>
LogicalCachedExtent* BtreeNodePin<key_t, val_t>::get_logical_extent(
  Transaction &t)
{
  assert(parent);
  assert(parent->is_valid());
  assert(pos != std::numeric_limits<uint16_t>::max());
  auto &p = (FixedKVNode<key_t>&)*parent;
  return p.get_logical_child(t, pos);
}

template void BtreeNodePin<laddr_t, paddr_t>::link_extent(LogicalCachedExtent*);
template void BtreeNodePin<paddr_t, laddr_t>::link_extent(LogicalCachedExtent*);
template LogicalCachedExtent* BtreeNodePin<laddr_t, paddr_t>::get_logical_extent(
  Transaction &t);
template LogicalCachedExtent* BtreeNodePin<paddr_t, laddr_t>::get_logical_extent(
  Transaction &t);
} // namespace crimson::os::seastore
