// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

/*
 * intra-fixedkv-btree design:
 *
 * To accelerate the search of the fixedkv-btree, we need to avoid searching
 * the whole Cache to get the child node.
 *
 * For the above purpose, we introduce the child_tracker_t which is used by
 * parent nodes to tracker their children. The relation between the parent node,
 * the child node and the tracker is as folows:
 *
 * 				----------
 * 				| parent |
 * 				----------
 * 			       /     |     \
 * 			      /      |      \
 * 		      ---------  ---------  ---------
 * 		      |tracker|  |tracker|  |tracker|
 * 		      ---------  ---------  ---------
 * 		         /           |           \
 * 		        /            |            \
 *		  ---------      ---------     ---------
 *		  | child |      | child |     | child |
 *                ---------      ---------     ---------
 * Basically, child_tracker_t is a weak ptr pointing to the corresponding child
 * node. The reason why we introduced child_tracker_t is to deal with following
 * two scenarios:
 * 1. A child has to be evict out of the cache, and there are multiple parents
 *    referencing it(one "clean/dirty" parent and several "pending" parents that
 *    are generated by transactions making changes) all of which has to be notified
 *    of the eviction.
 * 2. A child has to be loaded from disk by a transaction that are trying to
 *    reference this child through the "pending" parent it created(by mutating/splitting
 *    /merging/rebalancing the non-pending parents), since the child is loaded as
 *    CLEAN, it has to be visible to all other parents that should reference it as
 *    long as it's in memory.
 *
 * In scenario 1, child_tracker_t's weak ptr is cleared once the child is evicted,
 * so all of its parents are notified(They can no longer find it in memory).
 * In scenario 2, once the child is loaded, child_tracker_t's weak ptr is set,
 * so all other parents can find it.
 */

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

// once a fixedkv-btree node is duplicated for write, the new mutation pending node
// has to be added to its parent's child_trans_views set, so that the transaction
// can find it later when searching the fixedkv btree.
//
// NOTE: only CLEAN/DIRTY node may have per-transaction-view of its children, pending
// 	 nodes can only see its children in the view of its owning transaction.
struct child_trans_views_t {
  child_trans_views_t() = default;
  child_trans_views_t(size_t capacity)
    : views_by_transaction(capacity, std::nullopt) {}

  CachedExtent::trans_view_mset_t trans_views;
  std::vector<std::optional<std::map<transaction_id_t, CachedExtent*>>>
  views_by_transaction;

  CachedExtent* get_child_trans_view(Transaction &t, uint64_t pos);

  void new_trans_view(CachedExtent &child_tv, uint64_t pos) {
    ceph_assert(child_tv.pending_for_transaction);
    ceph_assert(views_by_transaction.capacity());
    trans_views.insert(child_tv);
    auto &v_by_t = views_by_transaction[pos];
    if (!v_by_t) {
      v_by_t = std::make_optional<std::map<transaction_id_t, CachedExtent*>>();
    }
    auto [iter, inserted] = v_by_t->emplace(child_tv.pending_for_transaction, &child_tv);
    ceph_assert(inserted);
  }

  template <typename T>
  std::list<std::pair<CachedExtent*, uint64_t>> remove_trans_view(Transaction &t);

  bool empty() {
    return trans_views.empty();
  }
};

// this is simply a pointer to a node's parent, it's used by mutation pending
// nodes to turn its transactional view into a global view when it has replaced
// the old one.
struct parent_tracker_t {
  parent_tracker_t(CachedExtent* parent, uint64_t pos)
    : parent(parent), pos(pos) {}
  CachedExtent* parent;
  uint64_t pos;
};
using parent_tracker_ref =
  std::unique_ptr<parent_tracker_t>;

template <typename key_t>
struct back_tracker_t : public boost::intrusive_ref_counter<
			back_tracker_t<key_t>, boost::thread_unsafe_counter> {
  back_tracker_t(CachedExtentRef parent)
    : parent(parent) {}
  CachedExtentRef parent;
  ~back_tracker_t();
};
template <typename key_t>
using back_tracker_ref =
  boost::intrusive_ptr<back_tracker_t<key_t>>;

} // namespace crimson::os::seastore
