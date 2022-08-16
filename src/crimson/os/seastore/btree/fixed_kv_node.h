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

struct parent_tracker_t {
  CachedExtent* parent;
  uint64_t pos;
};
using parent_tracker_ref =
  std::unique_ptr<parent_tracker_t>;

/**
 * FixedKVNode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
template <typename node_key_t>
struct FixedKVNode : CachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;

  btree_range_pin_t<node_key_t> pin;

  // these raw pointers are initialized when:
  // 	1. on_clean_read
  // 	2. new mapping is added
  // and are destroyed when:
  // 	1. the node is valid and evicted out of Cache
  // 	2. the mapping is removed from the node or replaced
  //
  // NOTE THAT: invalidating a node doesn't need to destory these raw
  // 		pointers, as there must be at least one pending node that's
  // 		referencing the corresponding child trackers
  std::vector<child_tracker_t*> child_trackers;
  trans_spec_view_t::trans_view_set_t child_trans_views;
  parent_tracker_ref parent_tracker;
  std::list<child_tracker_t*> trackers_to_rm;

  FixedKVNode(size_t capacity, ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)), pin(this), child_trackers(capacity, nullptr) {}
  FixedKVNode(const FixedKVNode &rhs)
    : CachedExtent(rhs), pin(rhs.pin, this), child_trackers(rhs.child_trackers) {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;

  virtual ~FixedKVNode() = default;

  void on_delta_commit(paddr_t record_block_offset) final {
    LOG_PREFIX(FixedKVNode::on_delta_commit);
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    pin.take_pin(get_prior_instance()->template cast<FixedKVNode>()->pin);
    resolve_relative_addrs(record_block_offset);
    for (auto tracker : trackers_to_rm) {
      SUBTRACE(seastore_fixedkv_tree,
	"delete tracker: {}, this: {}", (void*)tracker, *this);
      delete tracker;
    }
    trackers_to_rm.clear();
    if (parent_tracker) {
      // change my parent to point to me
      auto parent = parent_tracker->parent;
      ceph_assert(parent);
      auto tracker = ((FixedKVNode*)parent)->child_trackers[parent_tracker->pos];
      ceph_assert(child_trans_view_hook.is_linked());
      child_trans_view_hook.unlink();
      tracker->child = weak_from_this();
      parent_tracker.reset();
    }
  }

  void on_initial_commit() final {
    LOG_PREFIX(FixedKVNode::on_initial_commit);
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    for (auto tracker : trackers_to_rm) {
      SUBTRACE(seastore_fixedkv_tree,
	"delete tracker: {}, this: {}", (void*)tracker, *this);
      delete tracker;
    }
    trackers_to_rm.clear();
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    init_child_trackers();
  }

  virtual void init_child_trackers() = 0;
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
  using base_t = FixedKVNode<NODE_KEY>;
  using base_ref = base_t::FixedKVNodeRef;
  using node_layout_t =
    common::FixedKVNodeLayout<
      CAPACITY,
      fixed_kv_node_meta_t<NODE_KEY>,
      fixed_kv_node_meta_le_t<NODE_KEY_LE>,
      NODE_KEY,
      NODE_KEY_LE,
      paddr_t,
      paddr_le_t>;
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  using internal_iterator_t = typename node_layout_t::iterator;
  FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  void init_child_trackers() {
    LOG_PREFIX(FixedKVNode::init_child_trackers);
    for (auto it = this->child_trackers.begin();
	 it != this->child_trackers.begin() + this->get_size();
	 it++) {
      *it = new child_tracker_t();
      SUBTRACE(seastore_fixedkv_tree,
	"init tracker: {}, this: {}", (void*)(*it), *this);
    }
  }

  void link_child(CachedExtent &child, uint64_t pos) {
    auto tracker = this->child_trackers[pos];
    assert(tracker != nullptr);
    assert(!tracker->child
      || (tracker->child && tracker->child.get() == &child));
    //ceph_assert((!this->is_pending() && !child.is_pending())
    //  || (child.is_pending() && this->is_pending()));
    if (!tracker->child) {
      tracker->child = child.weak_from_this();
    }
  }

  void new_child(CachedExtent &child, uint64_t pos) {
    auto &tracker = this->child_trackers[pos];
    assert(tracker != nullptr);
    ceph_assert(child.is_mutation_pending() && this->is_pending());
    tracker = new child_tracker_t(&child);
  }

  void add_child_trans_view(FixedKVNode<NODE_KEY> &child, uint64_t pos) {
    this->child_trans_views.insert(child);
    ceph_assert(!this->is_pending() && child.is_mutation_pending());
    ceph_assert(!child.parent_tracker);
    ceph_assert(this->child_trackers[pos]);
    child.parent_tracker = std::make_unique<parent_tracker_t>(this, pos);
  }

  void copy_child_trackers_out(FixedKVInternalNode &new_node) {
    LOG_PREFIX(FixedKVInternalNode::copy_child_trackers_out);
    SUBTRACE(seastore_fixedkv_tree,
      "coping {} trackers from {} to {}",
      this->get_size(), *this, new_node);

    ceph_assert(this->get_type() == new_node.get_type());
    auto data = this->child_trackers.data();
    auto n_data = new_node.child_trackers.data();
#ifndef NDEBUG
    for (int i = 0; i < this->get_size(); i++) {
      assert(this->child_trackers[i]);
    }
#endif
    std::memmove(n_data, data, this->get_size() * sizeof(child_tracker_t*));
  }

  void split_child_trackers(
    Transaction &t,
    base_t &left,
    base_t &right)
  {
    LOG_PREFIX(FixedKVNode::split_child_trackers);
    size_t pivot = this->get_size() / 2;
    child_tracker_t** l_data = left.child_trackers.data();
    child_tracker_t** r_data = right.child_trackers.data();
    child_tracker_t** data = this->child_trackers.data();
    size_t l_size = pivot;
    size_t r_size = this->get_size() - pivot;

    std::memmove(l_data, data, sizeof(child_tracker_t*) * l_size);
    std::memmove(r_data, data + pivot, sizeof(child_tracker_t*) * r_size);

    SUBTRACET(seastore_fixedkv_tree,
      "l_size: {}, {}; r_size: {}, {}",
      t, l_size, left, r_size, right);
  }

  template <typename T1, typename T2>
  void merge_child_trackers(
    const T1 &left,
    const T2 &right)
  {
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T1>);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T2>);
    auto l_data = left.child_trackers.data();
    auto r_data = right.child_trackers.data();
    auto data = this->child_trackers.data();
    auto l_size = left.get_size();
    auto r_size = right.get_size();
    ceph_assert(l_size + r_size <= CAPACITY);

    std::memmove(data, l_data, l_size);
    std::memmove(data + l_size, r_data, r_size);
  }

  template <typename T1, typename T2>
  static void balance_child_trackers(
    Transaction &t,
    const T1 &left,
    const T2 &right,
    bool prefer_left,
    T2 &replacement_left,
    T2 &replacement_right)
  {
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T1>);
    static_assert(std::is_base_of_v<FixedKVNode<NODE_KEY>, T2>);
    auto l_size = left.get_size();
    auto r_size = right.get_size();
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

    auto l_data = left.child_trackers.data();
    auto r_data = right.child_trackers.data();
    auto rep_l_data = replacement_left.child_trackers.data();
    auto rep_r_data = replacement_right.child_trackers.data();

    if (pivot_idx < l_size) {
      std::memmove(rep_l_data, l_data, pivot_idx * sizeof(child_tracker_t*));
      std::memmove(rep_r_data, l_data + pivot_idx,
	(l_size - pivot_idx) * sizeof(child_tracker_t*));
      std::memmove(rep_r_data, r_data, r_size * sizeof(child_tracker_t*));
    } else {
      std::memmove(rep_l_data, l_data, l_size * sizeof(child_tracker_t*));
      std::memmove(rep_l_data, r_data,
	(pivot_idx - l_size) * sizeof(child_tracker_t*));
      std::memmove(rep_r_data, r_data + pivot_idx - l_size,
	(r_size + l_size - pivot_idx) * sizeof(child_tracker_t*));
    }
  }

  virtual ~FixedKVInternalNode() {
    LOG_PREFIX(FixedKVInternalNode::~FixedKVInternalNode);
    if (this->is_valid()) {
      for (auto it = this->child_trackers.begin();
	   it != this->child_trackers.begin() + this->get_size();
	   it++) {
	SUBTRACE(seastore_fixedkv_tree,
	  "delete tracker: {}, this: {}", (void*)*it, (void*)this);
	delete *it;
      }
    }
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef get_mutable_replica(Transaction &t) override {
    assert(delta_buffer.empty());
    auto ext = new node_type_t(*this);
    auto tid = t.get_trans_id();
    ext->touched_by = tid;
    auto iter = this->child_trans_views.lower_bound(
      tid,
      trans_spec_view_t::cmp_t());
    for (; iter->touched_by == tid;) {
      auto &child_trans_view = *iter;
      iter = this->child_trans_views.erase(iter);
      auto child_pos = ((base_t&)child_trans_view).parent_tracker->pos;
      ext->child_trackers[child_pos] =
	new child_tracker_t((CachedExtent*)&child_trans_view);
      ((base_t&)child_trans_view).parent_tracker.reset();
    }
    return ext;
  };

  void update(
    internal_const_iterator_t iter,
    paddr_t addr,
    CachedExtentRef new_node) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    ceph_assert(is_fixed_kv_node(new_node->get_type()));
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::update);

    this->trackers_to_rm.push_back(this->child_trackers[iter.get_offset()]);
    this->child_trackers[iter.get_offset()] = new child_tracker_t(new_node);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}, new tracker: {}, new extent: {}",
      (void*)&this->trackers_to_rm.back(),
      (void*)this->child_trackers[iter.get_offset()],
      *new_node);

    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void new_root(CachedExtentRef &old_root) {
    ceph_assert(this->get_meta().is_root());
    auto &tracker = this->child_trackers[0];
    ceph_assert(tracker == nullptr);
    tracker = new child_tracker_t(old_root);
  }

  void insert(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    CachedExtentRef new_node) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    ceph_assert(is_fixed_kv_node(new_node->get_type()));

    // move child trackers
    size_t count = sizeof(child_tracker_t*) * (
      this->get_size() - iter.get_offset());
    void* src = this->child_trackers.data() + iter.get_offset();
    void* dest = this->child_trackers.data() + iter.get_offset() + 1;
    std::memmove(dest, src, count);

    this->child_trackers[iter.get_offset()] = new child_tracker_t(new_node);
    LOG_PREFIX(FixedKVInternalNode::insert);
    SUBTRACE(seastore_fixedkv_tree,
      "new tracker: {}, new extent: {}",
      (void*)this->child_trackers[iter.get_offset()],
      *new_node);

    return this->journal_insert(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(internal_const_iterator_t iter) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::remove);

    this->trackers_to_rm.push_back(this->child_trackers[iter.get_offset()]);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}",
      (void*)&this->trackers_to_rm.back());

    // move child trackers
    size_t count = sizeof(child_tracker_t*) * (
      this->get_size() - iter.get_offset() - 1);
    void* src = this->child_trackers.data() + iter.get_offset() + 1;
    void* dest = this->child_trackers.data() + iter.get_offset();
    std::memmove(dest, src, count);

    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    CachedExtentRef new_node) {
    ceph_assert(this->child_trans_views.empty());
    ceph_assert(this->is_pending());
    assert(this->child_trackers[iter.get_offset()]);
    assert(this->child_trackers[iter.get_offset()]->child);
    LOG_PREFIX(FixedKVInternalNode::remove);

    this->trackers_to_rm.push_back(this->child_trackers[iter.get_offset()]);
    this->child_trackers[iter.get_offset()] = new child_tracker_t(new_node);
    SUBTRACE(seastore_fixedkv_tree,
      "old tracker: {}, new tracker: {}, new extent: {}",
      (void*)&this->trackers_to_rm.back(),
      (void*)this->child_trackers[iter.get_offset()],
      *new_node);

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
    this->split_child_trackers(c.trans, *left, *right);
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
    replacement->merge_child_trackers(*this, *right);
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
      *replacement_right);

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
	  i->set_val(i->get_val().block_relative_to(this->get_paddr()));
	}
      }
    }
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    return out << ", size=" << this->get_size()
	       << ", meta=" << this->get_meta();
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
    buffer.replay(
      *this,
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	size_t count = sizeof(child_tracker_t*) * (
	  this->get_size() - iter.get_offset());
	void* src = this->child_trackers.data() + iter.get_offset();
	void* dest = this->child_trackers.data() + iter.get_offset() + 1;
	std::memmove(dest, src, count);
	this->child_trackers[iter.get_offset()] = new child_tracker_t();
	SUBTRACE(seastore_fixedkv_tree, "insert pos {}, tracker: {}",
	  iter.get_offset(), (void*)this->child_trackers[iter.get_offset()]);
      },
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	SUBTRACE(seastore_fixedkv_tree, "remove pos {}, tracker: {}",
	  iter.get_offset(), (void*)this->child_trackers[iter.get_offset()]);
	size_t count = sizeof(child_tracker_t*) * (
	  this->get_size() - iter.get_offset() - 1);
	void* src = this->child_trackers.data() + iter.get_offset() + 1;
	void* dest = this->child_trackers.data() + iter.get_offset();
	delete this->child_trackers[iter.get_offset()];
	std::memmove(dest, src, count);
      },
      [this](auto &iter) {
	LOG_PREFIX(FixedKVInternalNode::apply_delta_and_adjust_crc);
	auto tracker = this->child_trackers[iter.get_offset()];
	delete tracker;
	this->child_trackers[iter.get_offset()] = new child_tracker_t();
	SUBTRACE(seastore_fixedkv_tree, "update pos {}, old tracker: {}, new: {}",
	  iter.get_offset(), (void*)tracker,
	  (void*)this->child_trackers[iter.get_offset()]);
      });
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
  using base_t = FixedKVNode<NODE_KEY>;
  using base_ref = base_t::FixedKVNodeRef;
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
  using internal_const_iterator_t = typename node_layout_t::const_iterator;

  FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  void init_child_trackers() {
    //TODO: noop for now, as we are only dealing with intra-fixed-kv-btree
    //	    node trackers now.
    //	    In the future, when linking logical extents with their parents,
    //	    this method should be implemented.
  }

  template <typename... T>
  FixedKVLeafNode(T&&... t) :
    FixedKVNode<NODE_KEY>(std::forward<T>(t)...),
    node_layout_t(this->get_bptr().c_str()) {}

  virtual ~FixedKVLeafNode() {}

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  virtual typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() ? &delta_buffer : nullptr;
  }

  CachedExtentRef get_mutable_replica(Transaction &t) override {
    auto ext = CachedExtentRef(new node_type_t(*this));
    ext->touched_by = t.get_trans_id();
    return ext;
  };

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
    buffer.replay(*this, [](auto&) {}, [](auto&) {}, [](auto&) {});
    this->set_last_committed_crc(this->get_crc32c());
    this->resolve_relative_addrs(base);
  }

  std::ostream &print_detail(std::ostream &out) const
  {
    return out << ", size=" << this->get_size()
	       << ", meta=" << this->get_meta();
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
