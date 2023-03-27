// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

template <typename key_t, typename val_t>
void BtreeNodeMapping<key_t, val_t>::init_child_pos(
  Transaction &t)
{
  assert(parent);
  assert(parent->is_valid());
  assert(pos != std::numeric_limits<uint16_t>::max());
  auto &p = (FixedKVNode<key_t>&)*parent;
  this->child_pos = p.get_logical_child(t, pos);
}

template void BtreeNodeMapping<laddr_t, paddr_t>::init_child_pos(Transaction &t);
template void BtreeNodeMapping<paddr_t, laddr_t>::init_child_pos(Transaction &t);
} // namespace crimson::os::seastore
