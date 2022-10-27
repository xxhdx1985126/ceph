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
#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

/**
 * FixedKVNode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
template <typename node_key_t>
struct FixedKVNode : ChildableCachedExtent {
  using FixedKVNodeRef = TCachedExtentRef<FixedKVNode>;
  btree_range_pin_t<node_key_t> pin;

  void take_children(FixedKVNode &other, uint16_t start, uint16_t end) {
    ceph_assert(start < end);
    for (auto it = other.children.begin() + start;
	it != other.children.begin() + end;
	it++)
    {
      auto child = *it;
      if (child) {
	set_child_ptracker(child);
      }
    }
  }

  struct copy_source_cmp_t {
    using is_transparent = node_key_t;
    bool operator()(const FixedKVNodeRef &l, const FixedKVNodeRef &r) const {
      assert(l->pin.range.end <= r->pin.range.begin
	|| r->pin.range.end <= l->pin.range.begin
	|| (l->pin.range.begin == r->pin.range.begin
	    && l->pin.range.end == r->pin.range.end));
      return l->pin.range.begin < r->pin.range.begin;
    }
    bool operator()(const node_key_t &l, const FixedKVNodeRef &r) const {
      return l < r->pin.range.begin;
    }
    bool operator()(const FixedKVNodeRef &l, const node_key_t &r) const {
      return l->pin.range.begin < r;
    }
  };

  std::vector<ChildableCachedExtent*> children;
  std::set<FixedKVNodeRef, copy_source_cmp_t> copy_sources;
  uint16_t capacity = 0;
  parent_tracker_t* my_tracker = nullptr;
  RootBlockRef root_block;

  FixedKVNode(uint16_t capacity, ceph::bufferptr &&ptr)
    : ChildableCachedExtent(std::move(ptr)),
      pin(this),
      children(capacity, nullptr),
      capacity(capacity) {}
  FixedKVNode(const FixedKVNode &rhs)
    : ChildableCachedExtent(rhs),
      pin(rhs.pin, this),
      children(rhs.capacity, nullptr),
      capacity(rhs.capacity) {}

  virtual fixed_kv_node_meta_t<node_key_t> get_node_meta() const = 0;
  virtual uint16_t get_node_size() const = 0;

  virtual ~FixedKVNode() = default;
  virtual node_key_t get_key_from_idx(uint16_t idx) const = 0;

  template<typename iter_t>
  void update_children(iter_t iter, ChildableCachedExtent* child) {
    children[iter.get_offset()] = child;
  }

  template<typename iter_t>
  void insert_children(iter_t iter, ChildableCachedExtent* child) {
    auto raw_children = children.data();
    auto offset = iter.get_offset();
    std::memmove(
      &raw_children[offset + 1],
      &raw_children[offset],
      (get_node_size() - offset) * sizeof(ChildableCachedExtent*));
    children[offset] = child;
  }

  template<typename iter_t>
  void remove_children(iter_t iter) {
    LOG_PREFIX(FixedKVNode::remove_children);
    auto raw_children = children.data();
    auto offset = iter.get_offset();
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, total size {}",
      this->pending_for_transaction,
      offset,
      get_node_size());
    std::memmove(
      &raw_children[offset],
      &raw_children[offset + 1],
      (get_node_size() - offset - 1) * sizeof(ChildableCachedExtent*));
  }

  FixedKVNode& get_stable_for_key(node_key_t key) {
    ceph_assert(is_pending());
    if (is_mutation_pending()) {
      return (FixedKVNode&)*get_prior_instance();
    } else {
      ceph_assert(!copy_sources.empty());
      auto it = copy_sources.upper_bound(key);
      it--;
      auto &copy_source = *it;
      ceph_assert(copy_source->get_node_meta().is_in_range(key));
      return *copy_source;
    }
  }

  static void push_copy_sources(
    FixedKVNode &dest,
    FixedKVNode &src)
  {
    ceph_assert(dest.is_pending());
    if (!src.is_pending()) {
      dest.copy_sources.emplace(&src);
    } else if (src.is_mutation_pending()) {
      dest.copy_sources.emplace(
	src.get_prior_instance()->template cast<FixedKVNode>());
    } else {
      ceph_assert(src.is_initial_pending());
      dest.copy_sources.insert(
	src.copy_sources.begin(),
	src.copy_sources.end());
    }
  }

  virtual uint16_t get_node_split_pivot() = 0;

  static void move_children(
    FixedKVNode &dest,
    FixedKVNode &src,
    size_t dest_start,
    size_t src_start,
    size_t src_end)
  {
    std::memmove(
      dest.children.data() + dest_start,
      src.children.data() + src_start,
      (src_end - src_start) * sizeof(ChildableCachedExtent*));
    dest.take_children(src, src_start, src_end);
  }

  void link_child(ChildableCachedExtent* child, uint16_t pos) {
    assert(pos < get_node_size());
    assert(child);
    ceph_assert(!is_pending());
    ceph_assert(child->is_valid() && !child->is_pending());
    assert(!children[pos]);
    children[pos] = child;
    set_child_ptracker(child);
  }

  virtual child_pos_t get_logical_child(
    Transaction &t,
    uint16_t pos) = 0;

  template <typename iter_t>
  child_pos_t get_child(Transaction &t, iter_t iter) {
    auto pos = iter.get_offset();
    assert(children.capacity());
    auto child = children[pos];
    if (child) {
      return child_pos_t(child->get_transactional_view(t));
    } else if (is_pending()) {
      auto key = iter.get_key();
      auto &sparent = get_stable_for_key(key);
      auto spos = sparent.child_pos_for_key(key);
      auto child = sparent.children[spos];
      if (child) {
	return child_pos_t(child->get_transactional_view(t));
      } else {
	return child_pos_t(&sparent, spos);
      }
    } else {
      return child_pos_t(this, pos);
    }
  }

  void split_mutate_state(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    assert(!left.my_tracker);
    assert(!right.my_tracker);
    push_copy_sources(left, *this);
    push_copy_sources(right, *this);
    if (is_pending()) {
      uint16_t pivot = get_node_split_pivot();
      move_children(left, *this, 0, 0, pivot);
      move_children(right, *this, 0, pivot, get_node_size());
      my_tracker = nullptr;
    }
  }

  void merge_mutate_state(
    FixedKVNode &left,
    FixedKVNode &right)
  {
    ceph_assert(!my_tracker);
    push_copy_sources(*this, left);
    push_copy_sources(*this, right);

    if (left.is_pending()) {
      move_children(*this, left, 0, 0, left.get_node_size());
      left.my_tracker = nullptr;
    }

    if (right.is_pending()) {
      move_children(*this, right, left.get_node_size(), 0, right.get_node_size());
      right.my_tracker = nullptr;
    }
  }

  static void balance_mutate_state(
    FixedKVNode &left,
    FixedKVNode &right,
    bool prefer_left,
    FixedKVNode &replacement_left,
    FixedKVNode &replacement_right)
  {
    size_t l_size = left.get_node_size();
    size_t r_size = right.get_node_size();
    size_t total = l_size + r_size;
    size_t pivot_idx = (l_size + r_size) / 2;
    if (total % 2 && prefer_left) {
      pivot_idx++;
    }

    assert(!replacement_left.my_tracker);
    assert(!replacement_right.my_tracker);
    if (pivot_idx < l_size) {
      // deal with left
      push_copy_sources(replacement_left, left);
      push_copy_sources(replacement_right, left);
      if (left.is_pending()) {
	move_children(replacement_left, left, 0, 0, pivot_idx);
	move_children(replacement_right, left, 0, pivot_idx, l_size);
	left.my_tracker = nullptr;
      }

      // deal with right
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	move_children(replacement_right, right, l_size - pivot_idx, 0, r_size);
	right.my_tracker= nullptr;
      }
    } else {
      // deal with left
      push_copy_sources(replacement_left, left);
      if (left.is_pending()) {
	move_children(replacement_left, left, 0, 0, l_size);
	left.my_tracker = nullptr;
      }

      // deal with right
      push_copy_sources(replacement_left, right);
      push_copy_sources(replacement_right, right);
      if (right.is_pending()) {
	move_children(replacement_left, right, l_size, 0, pivot_idx - l_size);
	move_children(replacement_right, right, 0, pivot_idx - l_size, r_size);
	right.my_tracker= nullptr;
      }
    }
  }

  void set_parent_tracker() {
    assert(is_mutation_pending());
    auto &prior = (FixedKVNode&)(*get_prior_instance());
    if (pin.is_root()) {
      ceph_assert(prior.root_block);
      ceph_assert(pending_for_transaction);
      root_block = (RootBlock*)prior.root_block->get_transactional_view(
	pending_for_transaction);
      set_phy_tree_root_node<true, node_key_t>(root_block, this);
      return;
    }
    ceph_assert(!root_block);
    parent_tracker = prior.parent_tracker;
    assert(parent_tracker->get_parent<FixedKVNode>());
    auto parent = parent_tracker->get_parent<FixedKVNode>();
    assert(parent->is_valid());
    //TODO: can this search be avoided?
    auto off = parent->lower_bound_offset(get_node_meta().begin);
    assert(parent->get_key_from_idx(off) == get_node_meta().begin);
    parent->children[off] = this;
  }

  bool empty_stable_children() {
    for (auto it = children.begin();
	it != children.begin() + get_node_size();
	it++) {
      if (*it != nullptr) {
	return false;
      }
    }
    return true;
  }

  void take_prior_tracker() {
    assert(get_prior_instance());
    auto &prior = (FixedKVNode&)(*get_prior_instance());

    if (prior.my_tracker) {
      prior.my_tracker->reset_parent(this);
      my_tracker = prior.my_tracker;
      adjust_ptracker_for_pending_children();
    }
    assert(my_tracker || prior.empty_stable_children());
  }

  void adjust_ptracker_for_pending_children() {
    auto begin = children.begin();
    auto end = begin + get_node_size();
    for (auto it = begin; it != end; it++) {
      auto child = *it;
      if (child) {
	set_child_ptracker(child);
      }
    }
  }

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    assert(pending_for_transaction);
    pin.take_pin(get_prior_instance()->template cast<FixedKVNode>()->pin);
    resolve_relative_addrs(record_block_offset);
  }

  virtual uint16_t lower_bound_offset(node_key_t) const = 0;
  virtual uint16_t upper_bound_offset(node_key_t) const = 0;
  virtual uint16_t child_pos_for_key(node_key_t) const = 0;

  virtual bool validate_stable_children() = 0;

  template<typename iter_t>
  uint16_t copy_from_src_and_apply_mutates(
    FixedKVNode &source,
    iter_t foreign_start_it,
    iter_t foreign_end_it,
    iter_t local_start_it) {
    auto foreign_it = foreign_start_it, local_it = local_start_it;
    while (foreign_it != foreign_end_it
	  && local_it.get_offset() < get_node_size())
    {
      auto &child = children[local_it.get_offset()];
      if (foreign_it.get_key() == local_it.get_key()) {
	// the foreign key is preserved
	if (!child) {
	  child = source.children[foreign_it.get_offset()];
	}
	foreign_it++;
	local_it++;
      } else if (foreign_it.get_key() < local_it.get_key()) {
	// the foreign key has been removed, because, if it hasn't,
	// there must have been a local key before the one pointed
	// by the current "local_it" that's equal to this foreign key
	// and has pushed the foreign_it forward.
	foreign_it++;
      } else {
	// the local key must be a newly inserted one.
	local_it++;
      }
    }
    return local_it.get_offset();
  }

  template<typename Func>
  void copy_from_srcs_and_apply_mutates(Func &&get_iter) {
    if (!copy_sources.empty()) {
      auto it = --copy_sources.upper_bound(get_node_meta().begin);
      auto &cs = *it;
      uint16_t start_pos = cs->lower_bound_offset(
	get_node_meta().begin);
      if (start_pos == cs->get_node_size()) {
	it++;
	start_pos = 0;
      }
      uint16_t local_next_pos = 0;
      for (; it != copy_sources.end(); it++) {
	auto& copy_source = *it;
	auto end_pos = copy_source->get_node_size();
	if (copy_source->get_node_meta().is_in_range(get_node_meta().end)) {
	  end_pos = copy_source->upper_bound_offset(get_node_meta().end);
	}
	auto local_start_iter = get_iter(*this, local_next_pos);
	auto foreign_start_iter = get_iter(*copy_source, start_pos);
	auto foreign_end_iter = get_iter(*copy_source, end_pos);
	local_next_pos = copy_from_src_and_apply_mutates(
	  *copy_source, foreign_start_iter, foreign_end_iter, local_start_iter);
	if (end_pos != copy_source->get_node_size()) {
	  break;
	}
	start_pos = 0;
      }
    }
  }

  void on_invalidated(Transaction &t) final {
    if (trans_view_hook.is_linked()) {
      trans_view_hook.unlink();
    }
    parent_tracker.reset();
  }

  virtual void adjust_ptracker_for_stable_children() = 0;

  bool is_rewrite_from_mutation_pending() {
    return is_initial_pending() && get_prior_instance();
  }

  virtual void on_fixed_kv_node_initial_write() = 0;
  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
    if (pin.is_root()) {
      parent_tracker.reset();
    }
    assert(parent_tracker
	? (parent_tracker->get_parent()
	  && parent_tracker->get_parent()->is_valid())
	: true);
    on_fixed_kv_node_initial_write();
  }

  void set_child_ptracker(ChildableCachedExtent *child) {
    if (!this->my_tracker) {
      this->my_tracker = new parent_tracker_t(this);
    }
    child->reset_parent_tracker(this->my_tracker);
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
  using base_t = FixedKVNode<NODE_KEY>;
  using base_ref = typename FixedKVNode<NODE_KEY>::FixedKVNodeRef;
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
  using this_type_t = FixedKVInternalNode<
    CAPACITY,
    NODE_KEY,
    NODE_KEY_LE,
    node_size,
    node_type_t>;

  FixedKVInternalNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(CAPACITY, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVInternalNode(const FixedKVInternalNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  uint16_t get_node_split_pivot() final {
    return this->get_split_pivot().get_offset();
  }

  void on_fixed_kv_node_initial_write() final {
    if (this->is_rewrite_from_mutation_pending()) {
      this->take_prior_tracker();
    }
    this->copy_from_srcs_and_apply_mutates(
      [this](base_t &node, uint16_t pos) {
	ceph_assert(node.get_type() == this->get_type());
	auto &n = static_cast<this_type_t&>(node);
	return n.iter_idx(pos);
      }
    );
    if (this->is_rewrite_from_mutation_pending()) {
      this->reset_prior_instance();
    } else {
      this->adjust_ptracker_for_stable_children();
    }
    assert(this->validate_stable_children());
    this->copy_sources.clear();
  }

  child_pos_t get_logical_child(Transaction &, uint16_t) final
  {
    ceph_abort("impossible");
    return child_pos_t(nullptr);
  }

  void adjust_ptracker_for_stable_children() final {
    for (auto it = this->children.begin();
	it != this->children.begin() + this->get_size() &&
	  it != this->children.end();
	it++) {
      auto child = *it;
      if (!child) {
	continue;
      }
      this->set_child_ptracker(child);
    }
  }

  bool validate_stable_children() final {
    LOG_PREFIX(FixedKVInternalNode::validate_stable_children);
    if (this->children.empty()) {
      return false;
    }

    for (auto i : *this) {
      auto child = (FixedKVNode<NODE_KEY>*)this->children[i.get_offset()];
      if (child && !child->is_clean_pending()
	  && child->get_node_meta().begin != i.get_key()) {
	SUBERROR(seastore_fixedkv_tree,
	  "stable child not valid: child {}, child meta{}, key {}",
	  *child,
	  child->get_node_meta(),
	  i.get_key());
	ceph_abort();
	return false;
      }
    }
    return true;
  }

  virtual ~FixedKVInternalNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->pin.is_root()) {
	ceph_assert(this->root_block);
	set_phy_tree_root_node<false, NODE_KEY>(
	  this->root_block, (FixedKVNode<NODE_KEY>*)nullptr);
      } else {
	ceph_assert(this->parent_tracker);
	assert(this->parent_tracker->get_parent());
	auto parent = this->parent_tracker->template get_parent<
	  FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->children[off] == this);
	parent->children[off] = nullptr;
      }
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t upper_bound_offset(NODE_KEY key) const final {
    return this->upper_bound(key).get_offset();
  }

  uint16_t child_pos_for_key(NODE_KEY key) const final {
    auto it = this->upper_bound(key);
    assert(it != this->begin());
    --it;
    return it.get_offset();
  }

  NODE_KEY get_key_from_idx(uint16_t idx) const final {
    return this->iter_idx(idx).get_key();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint16_t get_node_size() const final {
    return this->get_size();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() 
	    ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    assert(delta_buffer.empty());
    return CachedExtentRef(new node_type_t(*this));
  };

  void on_replace_prior(Transaction &t) final {
    this->take_prior_tracker();
    auto &prior = (this_type_t&)(*this->get_prior_instance());
    auto copied = this->copy_from_src_and_apply_mutates(
      prior,
      prior.begin(),
      prior.end(),
      this->begin());
    ceph_assert(copied <= get_node_size());
    assert(this->validate_stable_children());
    this->set_parent_tracker();
  }

  void update(
    internal_const_iterator_t iter,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::update);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      *nextent);
    this->update_children(iter, nextent);
    this->set_child_ptracker(nextent);
    return this->journal_update(
      iter,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void insert(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::insert);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      pivot,
      *nextent);
    this->insert_children(iter, nextent);
    this->set_child_ptracker(nextent);
    return this->journal_insert(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  void remove(internal_const_iterator_t iter) {
    LOG_PREFIX(FixedKVInternalNode::remove);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, key {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key());
    this->remove_children(iter);
    return this->journal_remove(
      iter,
      maybe_get_delta_buffer());
  }

  void replace(
    internal_const_iterator_t iter,
    NODE_KEY pivot,
    paddr_t addr,
    FixedKVNode<NODE_KEY>* nextent) {
    LOG_PREFIX(FixedKVInternalNode::replace);
    SUBTRACE(seastore_fixedkv_tree, "trans.{}, pos {}, old key {}, key {}, {}",
      this->pending_for_transaction,
      iter.get_offset(),
      iter.get_key(),
      pivot,
      *nextent);
    this->update_children(iter, nextent);
    this->set_child_ptracker(nextent);
    return this->journal_replace(
      iter,
      pivot,
      this->maybe_generate_relative(addr),
      maybe_get_delta_buffer());
  }

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    this->split_mutate_state(*left, *right);
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
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    replacement->merge_mutate_state(*this, *right);
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
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    this->balance_mutate_state(
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
    out << ", size=" << this->get_size()
	<< ", meta=" << this->get_meta()
	<< ", parent_tracker=" << (void*)this->parent_tracker.get();
    if (this->parent_tracker) {
      out << ", parent=" << (void*)this->parent_tracker->get_parent().get();
    }
    out << ", my_tracker=" << (void*)this->my_tracker;
    if (this->my_tracker) {
      out << ", my_tracker->parent=" << (void*)this->my_tracker->get_parent().get();
    }
    return out << ", root_block=" << (void*)this->root_block.get();
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
  typename node_type_t,
  bool has_children>
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
  using internal_const_iterator_t = typename node_layout_t::const_iterator;
  using this_type_t = FixedKVLeafNode<
    CAPACITY,
    NODE_KEY,
    NODE_KEY_LE,
    VAL,
    VAL_LE,
    node_size,
    node_type_t,
    has_children>;
  using base_t = FixedKVNode<NODE_KEY>;
  FixedKVLeafNode(ceph::bufferptr &&ptr)
    : FixedKVNode<NODE_KEY>(has_children ? CAPACITY : 0, std::move(ptr)),
      node_layout_t(this->get_bptr().c_str()) {}
  FixedKVLeafNode(const FixedKVLeafNode &rhs)
    : FixedKVNode<NODE_KEY>(rhs),
      node_layout_t(this->get_bptr().c_str()) {}

  static constexpr bool do_has_children = has_children;

  uint16_t get_node_split_pivot() final {
    return this->get_split_pivot().get_offset();
  }

  child_pos_t get_logical_child(Transaction &t, uint16_t pos) final {
    auto child = this->children[pos];
    if (child) {
      return child_pos_t(child->get_transactional_view(t));
    } else if (this->is_pending()) {
      auto key = this->iter_idx(pos).get_key();
      auto &sparent = this->get_stable_for_key(key);
      auto spos = sparent.child_pos_for_key(key);
      auto child = sparent.children[spos];
      if (child) {
	return child_pos_t(child->get_transactional_view(t));
      } else {
	return child_pos_t(&sparent, spos);
      }
    } else {
      return child_pos_t(this, pos);
    }
  }

  void adjust_ptracker_for_stable_children() final {
    for (auto it = this->children.begin();
	it != this->children.begin() + this->get_size() &&
	  it != this->children.end();
	it++) {
      auto child = *it;
      if (!child) {
	continue;
      }
      this->set_child_ptracker(child);
    }
  }

  bool validate_stable_children() override {
    return true;
  }

  virtual ~FixedKVLeafNode() {
    if (this->is_valid() && !this->is_pending()) {
      if (this->pin.is_root()) {
	ceph_assert(this->root_block);
	set_phy_tree_root_node<false, NODE_KEY>(
	  this->root_block, (FixedKVNode<NODE_KEY>*)nullptr);
      } else {
	ceph_assert(this->parent_tracker);
	assert(this->parent_tracker->get_parent());
	auto parent = this->parent_tracker->template get_parent<
	  FixedKVNode<NODE_KEY>>();
	auto off = parent->lower_bound_offset(this->get_meta().begin);
	assert(parent->get_key_from_idx(off) == this->get_meta().begin);
	assert(parent->children[off] == this);
	parent->children[off] = nullptr;
      }
    }
  }

  void on_fixed_kv_node_initial_write() final {
    if constexpr (has_children) {
      if (this->is_rewrite_from_mutation_pending()) {
	this->take_prior_tracker();
      }
      this->copy_from_srcs_and_apply_mutates(
	[this](base_t &node, uint16_t pos) {
	  ceph_assert(node.get_type() == this->get_type());
	  auto &n = static_cast<this_type_t&>(node);
	  return n.iter_idx(pos);
	}
      );
      if (this->is_rewrite_from_mutation_pending()) {
	this->reset_prior_instance();
      } else {
	this->adjust_ptracker_for_stable_children();
      }
      assert(this->validate_stable_children());
      this->copy_sources.clear();
    }
    assert(this->copy_sources.empty());
  }

  void on_replace_prior(Transaction &t) final {
    if constexpr (has_children) {
      this->take_prior_tracker();
      auto &prior = (this_type_t&)(*this->get_prior_instance());
      auto copied = this->copy_from_src_and_apply_mutates(
	prior,
	prior.begin(),
	prior.end(),
	this->begin());
      ceph_assert(copied <= get_node_size());
      assert(this->validate_stable_children());
      this->set_parent_tracker();
    } else {
      this->set_parent_tracker();
    }
  }

  uint16_t lower_bound_offset(NODE_KEY key) const final {
    return this->lower_bound(key).get_offset();
  }

  uint16_t upper_bound_offset(NODE_KEY key) const final {
    return this->upper_bound(key).get_offset();
  }

  uint16_t child_pos_for_key(NODE_KEY key) const final {
    return lower_bound_offset(key);
  }

  NODE_KEY get_key_from_idx(uint16_t idx) const final {
    return this->iter_idx(idx).get_key();
  }

  fixed_kv_node_meta_t<NODE_KEY> get_node_meta() const {
    return this->get_meta();
  }

  uint16_t get_node_size() const final {
    return this->get_size();
  }

  typename node_layout_t::delta_buffer_t delta_buffer;
  virtual typename node_layout_t::delta_buffer_t *maybe_get_delta_buffer() {
    return this->is_mutation_pending() ? &delta_buffer : nullptr;
  }

  CachedExtentRef duplicate_for_write(Transaction&) override {
    assert(delta_buffer.empty());
    return CachedExtentRef(new node_type_t(*this));
  };

  virtual void update(
    internal_const_iterator_t iter,
    VAL val,
    LogicalCachedExtent* nextent) = 0;
  virtual internal_const_iterator_t insert(
    internal_const_iterator_t iter,
    NODE_KEY addr,
    VAL val,
    LogicalCachedExtent* nextent) = 0;
  virtual void remove(internal_const_iterator_t iter) = 0;

  std::tuple<Ref, Ref, NODE_KEY>
  make_split_children(op_context_t<NODE_KEY> c) {
    auto left = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    if constexpr (has_children) {
      this->split_mutate_state(*left, *right);
    }
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
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    if constexpr (has_children) {
      replacement->merge_mutate_state(*this, *right);
    }
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
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);
    auto replacement_right = c.cache.template alloc_new_extent<node_type_t>(
      c.trans, node_size, placement_hint_t::HOT, INIT_GENERATION);

    auto pivot = this->balance_into_new_nodes(
      *this,
      right,
      prefer_left,
      *replacement_left,
      *replacement_right);
    if constexpr (has_children) {
      this->balance_mutate_state(
	*this,
	right,
	prefer_left,
	*replacement_left,
	*replacement_right);
    }

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
	<< ", meta=" << this->get_meta()
	<< ", parent_tracker=" << (void*)this->parent_tracker.get();
    if (this->parent_tracker) {
      out << ", parent=" << (void*)this->parent_tracker->get_parent().get();
    }
    return out;
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
