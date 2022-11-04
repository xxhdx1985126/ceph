// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore::lba_manager::btree {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.paddr
             << "~" << v.len
             << ", refcount=" << v.refcount
             << ", checksum=" << v.checksum
             << ")";
}

template <bool has_children>
std::ostream &LBALeafNode<has_children>::print_detail(std::ostream &out) const
{
  out << ", size=" << this->get_size()
      << ", meta=" << this->get_meta()
      << ", parent_tracker=" << (void*)this->parent_tracker.get();
  if (this->parent_tracker) {
    return out << ", parent=" << (void*)this->parent_tracker->parent.get();
  }
  out << ", my_tracker=" << (void*)this->my_tracker;
  if (this->my_tracker) {
    out << ", my_tracker->parent=" << (void*)this->my_tracker->parent.get();
  }
  return out << ", root_block=" << (void*)this->root_block.get();
}

template std::ostream &LBALeafNode<true>::print_detail(std::ostream &out) const;
template std::ostream &LBALeafNode<false>::print_detail(std::ostream &out) const;

template <bool has_children>
void LBALeafNode<has_children>::resolve_relative_addrs(paddr_t base)
{
  LOG_PREFIX(LBALeafNode::resolve_relative_addrs);
  for (auto i: *this) {
    if (i->get_val().paddr.is_relative()) {
      auto val = i->get_val();
      val.paddr = base.add_relative(val.paddr);
      TRACE("{} -> {}", i->get_val().paddr, val.paddr);
      i->set_val(val);
    }
  }
}

template void LBALeafNode<true>::resolve_relative_addrs(paddr_t base);
template void LBALeafNode<false>::resolve_relative_addrs(paddr_t base);

}
