// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

class Transaction;

struct child_tracker_t {
  CachedExtentWeakRef child;	// pointer to the child node

  child_tracker_t() = default;
  child_tracker_t(CachedExtentRef &cref)
    : child(cref->weak_from_this()) {}
  child_tracker_t(CachedExtent* c)
    : child(c->weak_from_this()) {}
  child_tracker_t(const child_tracker_t&) = delete;
  child_tracker_t(child_tracker_t&&) = delete;
};

std::ostream& operator<<(std::ostream&, child_tracker_t&);

struct child_trans_views_t {
  child_trans_views_t() = default;
  child_trans_views_t(size_t capacity)
    : views_by_transaction(capacity, std::nullopt) {}

  CachedExtent::trans_view_set_t trans_views;
  std::vector<std::optional<std::map<transaction_id_t, CachedExtent*>>>
  views_by_transaction;

  CachedExtent* get_child_trans_view(Transaction &t, uint64_t pos);

  void new_trans_view(CachedExtent &child_tv, uint64_t pos) {
    ceph_assert(child_tv.touched_by);
    ceph_assert(views_by_transaction.capacity());
    trans_views.insert(child_tv);
    auto &v_by_t = views_by_transaction[pos];
    if (!v_by_t) {
      v_by_t = std::make_optional<std::map<transaction_id_t, CachedExtent*>>();
    }
    auto [iter, inserted] = v_by_t->emplace(child_tv.touched_by, &child_tv);
    ceph_assert(inserted);
  }

  template <typename T>
  std::list<std::pair<CachedExtent*, uint64_t>> remove_trans_view(Transaction &t);

  bool empty() {
    return trans_views.empty();
  }
};

struct parent_tracker_t {
  parent_tracker_t(CachedExtent* parent, uint64_t pos)
    : parent(parent), pos(pos) {}
  CachedExtent* parent;
  uint64_t pos;
};
using parent_tracker_ref =
  std::unique_ptr<parent_tracker_t>;

} // namespace crimson::os::seastore
