// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace crimson::os::seastore {

void RootBlock::on_replace_prior(Transaction &t) {
  if (pending_lba_root_node) {
    lba_root_node = pending_lba_root_node;
    pending_lba_root_node = nullptr;
  } else {
    auto &prior = (RootBlock&)*get_prior_instance();
    lba_root_node = prior.lba_root_node;
  }
  if (lba_root_node) {
    auto &lba_root =
      (crimson::os::seastore::lba_manager::btree::LBANode&)*lba_root_node;
    lba_root.root_block = this;
  }

  if (pending_backref_root_node) {
    backref_root_node = pending_backref_root_node;
    pending_backref_root_node = nullptr;
  } else {
    auto &prior = (RootBlock&)*get_prior_instance();
    backref_root_node = prior.backref_root_node;
  }
  if (backref_root_node) {
    auto &backref_root =
      (crimson::os::seastore::backref::BackrefNode&)*backref_root_node;
    backref_root.root_block = this;
  }
}

} // namespace crimson::os::seastore
