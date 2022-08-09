// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>


#include "include/buffer.h"

#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"

#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"

namespace crimson::os::seastore {

/**
 * FixedKVNode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
template <typename node_key_t>
struct FixedKVNode : CachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;

  btree_range_pin_t<node_key_t> pin;
  std::vector<ChildNodeTrackerRef> child_trackers;
  size_t max_entries;
  // used to remember the pos in the parent node, should only
  // be non-nullptr if the current node is the root of the modified
  // subtree
  ChildNodeTracker* parent_tracker = nullptr;
  ChildNodeTracker::set_t parent_tracker_trans_views;

  FixedKVNode(size_t max_entries, ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)),
      pin(this),
      child_trackers(max_entries + 1),
      max_entries(max_entries)
  {}
  FixedKVNode(const FixedKVNode &rhs)
    : CachedExtent(rhs),
      pin(rhs.pin, this),
      child_trackers(rhs.max_entries + 1),
      max_entries(rhs.max_entries),
      parent_tracker(rhs.parent_tracker)
  {}

  std::ostream &dump_parent_tracker_trans_views(std::ostream &out) const {
    out << ", parent_tracker_trans_views(";
    for (auto &ptracker : parent_tracker_trans_views) {
      out << ptracker.get_parent_mutated_by() << "->" << ptracker << " ";
    }
    return out << ")";
  }

  void split_child_trackers(
    Transaction &t,
    FixedKVNode &left,
    FixedKVNode &right,
    size_t size)
  {
    LOG_PREFIX(FixedKVNode::split_child_trackers);
    auto split_pos = size / 2;
    auto l_it = left.child_trackers.begin();
    auto r_it = right.child_trackers.begin();
    auto my_mid_it = child_trackers.begin() + split_pos;
    size_t l_size = 0;
    size_t r_size = 0;
    //TODO: should avoid unnecessary copies, involve size;
    for (auto my_it = child_trackers.begin();
	 my_it != child_trackers.end();
	 my_it++) {
      if (my_it < my_mid_it) {
	if (*my_it) {
	  *l_it = std::make_unique<ChildNodeTracker>(**my_it, &left, t);
	}
	l_it++;
	l_size++;
      } else {
	if (*my_it) {
	  *r_it = std::make_unique<ChildNodeTracker>(**my_it, &right, t);
	}
	r_it++;
	r_size++;
      }
    }
    SUBTRACET(seastore_fixedkv_tree,
      "l_size: {}, {}; r_size: {}, {}",
      t, l_size, left, r_size, right);
  }

  void merge_child_trackers(
    Transaction &t,
    const FixedKVNode &left,
    const FixedKVNode &right)
  {
    auto l_it = left.child_trackers.begin();
    auto r_it = right.child_trackers.begin();
    //TODO: should avoid unnecessary copies, involve l/r_size;
    for (auto &tracker : child_trackers) {
      if (l_it != left.child_trackers.end()) {
	if (*l_it) {
	  tracker = std::make_unique<ChildNodeTracker>(**l_it, this, t);
	}
	l_it++;
      } else if (r_it != right.child_trackers.end()) {
	if (*r_it) {
	  tracker = std::make_unique<ChildNodeTracker>(**r_it, this, t);
	}
	r_it++;
      } else {
	break;
      }
    }
  }

  static void balance_child_trackers(
    Transaction &t,
    const FixedKVNode &left,
    const FixedKVNode &right,
    bool prefer_left,
    FixedKVNode &replacement_left,
    FixedKVNode &replacement_right,
    size_t l_size,
    size_t r_size)
  {
    auto total = l_size + r_size;
    auto pivot_idx = (l_size + r_size) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }
    LOG_PREFIX(FixedKVNode::balance_child_trackers);
    SUBTRACE(seastore_fixedkv_tree,
      "l_size: {}, r_size: {}, pivot_idx: {}",
      l_size,
      r_size,
      pivot_idx);
    if (pivot_idx < l_size) {
      auto r_l_it = replacement_left.child_trackers.begin();
      auto end_it = left.child_trackers.begin() + pivot_idx;
      for (auto it = left.child_trackers.begin(); it != end_it; r_l_it++, it++) {
	if (!(*it))
	  continue;
	*r_l_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_left, t);
      }
      auto r_r_it = replacement_right.child_trackers.begin();
      end_it = left.child_trackers.begin() + l_size;
      for (auto it = end_it; it != end_it; r_r_it++, it++) {
	if (!(*it))
	  continue;
	*r_r_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_right, t);
      }
      end_it = right.child_trackers.begin() + r_size;
      for (auto it = right.child_trackers.begin();
	   it != end_it; r_r_it++, it++) {
	if (!(*it))
	  continue;
	*r_r_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_right, t);
      }
    } else {
      auto r_l_it = replacement_left.child_trackers.begin();
      auto end_it = left.child_trackers.begin() + l_size;
      for (auto it = left.child_trackers.begin(); it != end_it; r_l_it++, it++) {
	if (!(*it))
	  continue;
	*r_l_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_left, t);
      }
      end_it = replacement_left.child_trackers.begin() + pivot_idx;
      for (auto it = right.child_trackers.begin();
	   r_l_it != end_it; r_l_it++, it++) {
	if (!(*it))
	  continue;
	*r_l_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_left, t);
      }
      auto r_r_it = replacement_right.child_trackers.begin();
      end_it = right.child_trackers.begin() + r_size;
      for (auto it = right.child_trackers.begin() + pivot_idx - l_size;
	   it != end_it;
	   r_r_it++, it++) {
	if (!(*it))
	  continue;
	*r_r_it = std::make_unique<ChildNodeTracker>(
	  **it, &replacement_right, t);
      }
    }
  }

  void add_child_tracker(CachedExtent* child, Transaction &t, uint64_t pos) {
    ceph_assert(pos < child_trackers.size());
    auto &tracker = child_trackers[pos];
    ceph_assert(!tracker->is_empty());
    ceph_assert(tracker);
    tracker->add_child_per_trans(child);
  }

  template<typename T>
  TCachedExtentRef<T> get_child(Transaction &t, uint64_t pos) {
    static_assert(std::is_base_of_v<FixedKVNode, T>);
    auto &tracker = child_trackers[pos];
    if (!tracker)
      return TCachedExtentRef<T>();
    auto child = tracker->get_child(t);
    if (child)
      return child->template cast<T>();
    else
      return TCachedExtentRef<T>();
  }

  ChildNodeTracker& get_child_tracker(uint64_t pos) {
    ceph_assert(pos < child_trackers.size());
    auto &tracker = child_trackers[pos];
    if (!tracker) {
      tracker = std::make_unique<ChildNodeTracker>(this);
    }
    return *tracker;
  }

  void move_to_trans_view(Transaction &t, FixedKVNode &orig, size_t size) {
    //TODO: may need perf improvement
    auto it_l = child_trackers.begin();
    auto end = orig.child_trackers.begin() + size;
    for (auto it_r = orig.child_trackers.begin(); it_r != end; it_r++) {
      auto &r_tracker = *it_r;
      if (r_tracker) {
	*it_l = std::make_unique<ChildNodeTracker>(*r_tracker, this, t);
      }
      it_l++;
    }
  }

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;
  virtual void unlink_from_children() = 0;

  void on_invalidated(Transaction &t) final {
    LOG_PREFIX(FixedKVNode::on_invalidated);
    ceph_assert(!is_valid());
    if (parent_tracker) {
      SUBTRACET(seastore_fixedkv_tree,
	"removing {} from {}",
	t, *this, *parent_tracker);
      parent_tracker->remove_child(this);
    }
    for (auto& ptracker : parent_tracker_trans_views) {
      ptracker.remove_child(&pin.get_extent());
    }
    parent_tracker_trans_views.clear();
    if (transaction_view_hook.is_linked()) {
      transaction_view_hook.unlink();
    }
    this->unlink_from_children();
  }

  virtual ~FixedKVNode() {
    if (is_valid()) {
      if (parent_tracker)
	parent_tracker->remove_child(this);
      for (auto& ptracker : parent_tracker_trans_views) {
	ptracker.remove_child(&pin.get_extent());
      }
      parent_tracker_trans_views.clear();
      if (transaction_view_hook.is_linked()) {
	transaction_view_hook.unlink();
      }
    }
  };

  virtual void adjust_child_parent_pointers() = 0;

  void on_delta_commit(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    pin.take_pin(get_prior_instance()->template cast<FixedKVNode>()->pin);
    resolve_relative_addrs(record_block_offset);
  }

  void on_replace_extent(Transaction &t, CachedExtent& prev) {
    if (parent_tracker)
      parent_tracker->on_transaction_commit(t);
    auto fixedkv_prev = prev.cast<FixedKVNode>();
    for (auto &ptracker : fixedkv_prev->parent_tracker_trans_views) {
      ptracker.update_child(this);
    }
    adjust_child_parent_pointers();
  }

  bool is_fixed_kv_btree_node() {
    return true;
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    adjust_child_parent_pointers();
    CachedExtent::on_initial_write();
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  virtual void resolve_relative_addrs(paddr_t base) = 0;
};

/**
 * FixedKVInternalNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * FixedKVBTree.
 */
template <
  size_t CAPACITY,
  typename NODE_KEY,
  typename NODE_KEY_LE,
  size_t node_size,
  typename node_type_t>
struct FixedKVInternalNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      paddr_t, paddr_le_t> {
  using Ref = TCachedExtentRef<node_type_t>;
  using node_layout_t =
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY,
      NODE_KEY_LE,
      paddr_t,
      paddr_le_t>;
  using base_ref_t = typename FixedKVNode<NODE_KEY>::FixedKVNodeRef;
  using base_t = FixedKVNode<NODE_KEY>;
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  using internal_iterator_t = typename node_layout_t::iterator;
  FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  virtual ~FixedKVInternalNode() {
    if (this->is_valid())
      unlink_from_children();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  void unlink_from_children() final {
    LOG_PREFIX(FixedKVInternalNode::unlink_from_children);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      auto &tracker = *it;
      if (tracker) {
	auto child = tracker->get_child_global_view();
	if (!child)
	  continue;
	SUBTRACE(seastore_fixedkv_tree,
	  "unlink from child {}, tracker: {}",
	  *child, *tracker);
	if (tracker->is_linked_to_child()) {
	  tracker->unlink_from_child();
	}
	ceph_assert(child->is_fixed_kv_btree_node());
	auto &ptracker = child->template cast<base_t>()->parent_tracker;
	if (ptracker == tracker.get())
	  ptracker = nullptr;
	tracker->remove_child(child);
      }
    }
  }

  void adjust_child_parent_pointers() final {
    LOG_PREFIX(FixedKVInternalNode::adjust_child_parent_pointers);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      auto &tracker = *it;
      if (tracker) {
	tracker->reset_prior_instance();
	if (!tracker->is_empty()) {
	  auto child = tracker->get_child_global_view();
	  if (!child)
	    continue;
	  SUBTRACE(seastore_fixedkv_tree,
	    "adjust child {}, parent tracker: {}",
	    *child, *tracker);
	  ceph_assert(child->is_fixed_kv_btree_node());
	  if (tracker->is_linked_to_child()) {
	    tracker->unlink_from_child();
	  }
	  child->template cast<base_t>()->parent_tracker =
	    tracker.get();
	}
      }
    }
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction &t) override {
    assert(delta_buffer.empty());
    auto new_node = new node_type_t(*this);
    new_node->mutated_by = t.get_trans_id();
    new_node->move_to_trans_view(t, *this, this->get_size());
    return CachedExtentRef(new_node);
  };

  void update(
    internal_const_iterator_t iter,
    paddr_t addr) {
    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void insert(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr) {
    for (auto it = this->child_trackers.begin() + this->get_size() - 1;
	 it >= this->child_trackers.begin() + iter.offset;
	 it--) {
      *(it + 1) = std::move(*it);
    }
    return this->journal_insert(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(internal_const_iterator_t iter) {
    for (auto it = this->child_trackers.begin() + iter.offset + 1;
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      *(it - 1) = std::move(*it);
    }
    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr) {
    return this->journal_replace(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    this->split_child_trackers(c.trans, *left, *right, this->get_size());
    auto pivot = this->split_into(*left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    replacement->merge_child_trackers(
      c.trans, *this, *right->template cast<node_type_t>());
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_balanced(
    op_context_t<NODE_KEY> c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == this->get_type());
    auto &right = *_right->template cast<node_type_t>();
    auto replacement_left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);

    this->balance_child_trackers(
      c.trans,
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right,
      this->get_size(),
      right.get_size());

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);

    replacement_left->pin.set_range(replacement_left->get_meta());
    replacement_right->pin.set_range(replacement_right->get_meta());
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  /**
   * Internal relative addresses on read or in memory prior to commit
   * are either record or block relative depending on whether this
   * physical node is is_initial_pending() or just is_pending().
   *
   * User passes appropriate base depending on lifecycle and
   * resolve_relative_addrs fixes up relative internal references
   * based on base.
   */
  void resolve_relative_addrs(paddr_t base)
  {
    LOG_PREFIX(FixedKVInternalNode::resolve_relative_addrs);
    for (auto i: *this) {
      if (i->get_val().is_relative()) {
	auto updated = base.add_relative(i->get_val());
	SUBTRACE(seastore_fixedkv_tree, "{} -> {}", i->get_val(), updated);
	i->set_val(updated);
      }
    }
  }

  void node_resolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_block_relative());
	  i->set_val(this->get_paddr().add_relative(i->get_val()));
	}
      }
    }
  }
  void node_unresolve_vals(
    internal_iterator_t from,
    internal_iterator_t to) const {
    if (this->is_initial_pending()) {
      for (auto i = from; i != to; ++i) {
	if (i->get_val().is_relative()) {
	  assert(i->get_val().is_record_relative());
	  i->set_val(i->get_val() - this->get_paddr());
	}
      }
    }
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
       << ", meta=" << this->get_meta();
    if (this->parent_tracker) {
      out << ", parent_tracker=" << *this->parent_tracker;
    } else {
      out << ", parent_tracker=0x0";
    }
    return this->dump_parent_tracker_trans_views(out);
  }

  ceph::bufferlist get_delta() {
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    typename node_layout_t::delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    this->set_last_committed_crc(this->get_crc32c());
    resolve_relative_addrs(base);
  }

  constexpr static size_t get_min_capacity() {
    return (node_layout_t::get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(this->get_size() <= node_layout_t::get_capacity());
    return this->get_size() == node_layout_t::get_capacity();
  }

  bool at_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() < get_min_capacity();
  }
};

template <
  size_t CAPACITY,
  typename NODE_KEY,
  typename NODE_KEY_LE,
  typename VAL,
  typename VAL_LE,
  size_t node_size,
  typename node_type_t>
struct FixedKVLeafNode
  : FixedKVNode<NODE_KEY>,
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY, NODE_KEY_LE,
      VAL, VAL_LE> {
  using Ref = TCachedExtentRef<node_type_t>;
  using node_layout_t =
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY,
      NODE_KEY_LE,
      VAL,
      VAL_LE>;
  using base_ref_t = typename FixedKVNode<NODE_KEY>::FixedKVNodeRef;
  using base_t = FixedKVNode<NODE_KEY>;
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  virtual ~FixedKVLeafNode() {
    if (this->is_valid())
      unlink_from_children();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  virtual typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction &t) override {
    assert(delta_buffer.empty());
    auto new_node = new node_type_t(*this);
    new_node->mutated_by = t.get_trans_id();
    new_node->move_to_trans_view(t, *this, this->get_size());
    return CachedExtentRef(new_node);
  };

  void unlink_from_children() final {
    LOG_PREFIX(FixedKVLeafNode::unlink_from_children);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      auto &tracker = *it;
      if (tracker) {
	auto child = tracker->get_child_global_view();
	if (!child)
	  continue;
	SUBTRACE(seastore_fixedkv_tree,
	  "unlink from child {}, tracker: {}",
	  *child, *tracker);
	if (tracker->is_linked_to_child()) {
	  tracker->unlink_from_child();
	}
	ceph_assert(child->is_logical());
	auto logical_child = child->template cast<LogicalCachedExtent>();
	auto &pin = logical_child->get_pin();
	pin.remove_parent_tracker(tracker.get());
	tracker->remove_child(child);
      }
    }
  }

  void adjust_child_parent_pointers() final {
    LOG_PREFIX(FixedKVLeafNode::adjust_child_parent_pointers);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      auto &tracker = *it;
      if (tracker) {
	tracker->reset_prior_instance();
	if (!tracker->is_empty()) {
	  auto child = tracker->get_child_global_view();
	  if (!child)
	    continue;
	  ceph_assert(child->is_logical());
	  SUBTRACE(seastore_fixedkv_tree,
	    "adjust child {}, parent tracker: {}",
	    *child, *tracker);
	  auto logical_child = child->template cast<LogicalCachedExtent>();
	  auto &pin = logical_child->get_pin();
	  if (tracker->is_linked_to_child()) {
	    tracker->unlink_from_child();
	  }
	  pin.new_parent_tracker(tracker.get());
	}
      }
    }
  }

  virtual void update(
    internal_const_iterator_t iter,
    VAL val) = 0;
  virtual internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    NODE_KEY addr,
    VAL val) = 0;
  virtual void remove(internal_const_iterator_t iter) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    this->split_child_trackers(c.trans, *left, *right, this->get_size());
    auto pivot = this->split_into(*left, *right);
    left->pin.set_range(left->get_meta());
    right->pin.set_range(right->get_meta());
    return std::make_tuple(
      left,
      right,
      pivot);
  }

  Ref make_full_merge(
    op_context_t<NODE_KEY> c,
    Ref &right) {
    auto replacement = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    replacement->merge_child_trackers(
      c.trans, *this, *right->template cast<node_type_t>());
    replacement->merge_from(*this, *right->template cast<node_type_t>());
    replacement->pin.set_range(replacement->get_meta());
    return replacement;
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_balanced(
    op_context_t<NODE_KEY> c,
    Ref &_right,
    bool prefer_left) {
    ceph_assert(_right->get_type() == this->get_type());
    auto &right = *_right->template cast<node_type_t>();
    auto replacement_left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, 0);

    this->balance_child_trackers(
      c.trans,
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right,
      this->get_size(),
      right.get_size());
     
    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);

    replacement_left->pin.set_range(replacement_left->get_meta());
    replacement_right->pin.set_range(replacement_right->get_meta());
    return std::make_tuple(
      replacement_left,
      replacement_right,
      pivot);
  }

  ceph::bufferlist get_delta() {
    ceph::buffer::ptr bptr(delta_buffer.get_bytes());
    delta_buffer.copy_out(bptr.c_str(), bptr.length());
    ceph::bufferlist bl;
    bl.push_back(bptr);
    return bl;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &_bl) {
    assert(_bl.length());
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    typename node_layout_t::delta_buffer_t buffer;
    buffer.copy_in(bl.front().c_str(), bl.front().length());
    buffer.replay(*this);
    this->set_last_committed_crc(this->get_crc32c());
    this->resolve_relative_addrs(base);
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    out << ", size=" << this->get_size()
       << ", meta=" << this->get_meta();
    if (this->parent_tracker) {
      out << ", parent_tracker=" << *this->parent_tracker;
    } else {
      out << ", parent_tracker=0x0";
    }
    return this->dump_parent_tracker_trans_views(out);
  }

  constexpr static size_t get_min_capacity() {
    return (node_layout_t::get_capacity() - 1) / 2;
  }

  bool at_max_capacity() const {
    assert(this->get_size() <= node_layout_t::get_capacity());
    return this->get_size() == node_layout_t::get_capacity();
  }

  bool at_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() <= get_min_capacity();
  }

  bool below_min_capacity() const {
    assert(this->get_size() >= (get_min_capacity() - 1));
    return this->get_size() < get_min_capacity();
  }
};

} // namespace crimson::os::seastore
