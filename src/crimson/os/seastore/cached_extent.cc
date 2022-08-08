// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore {

#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *ptr)
{
  intrusive_ptr_add_ref(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
    logger().debug("intrusive_ptr_add_ref: {}", *ptr);
}

void intrusive_ptr_release(CachedExtent *ptr)
{
  logger().debug("intrusive_ptr_release: {}", *ptr);
  intrusive_ptr_release(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
}

#endif

bool is_backref_mapped_extent_node(const CachedExtentRef &extent) {
  return extent->is_logical()
    || is_lba_node(extent->get_type())
    || extent->get_type() == extent_types_t::TEST_BLOCK_PHYSICAL;
}

std::ostream &operator<<(std::ostream &out, CachedExtent::extent_state_t state)
{
  switch (state) {
  case CachedExtent::extent_state_t::INITIAL_WRITE_PENDING:
    return out << "INITIAL_WRITE_PENDING";
  case CachedExtent::extent_state_t::MUTATION_PENDING:
    return out << "MUTATION_PENDING";
  case CachedExtent::extent_state_t::CLEAN_PENDING:
    return out << "CLEAN_PENDING";
  case CachedExtent::extent_state_t::CLEAN:
    return out << "CLEAN";
  case CachedExtent::extent_state_t::DIRTY:
    return out << "DIRTY";
  case CachedExtent::extent_state_t::EXIST_CLEAN:
    return out << "EXIST_CLEAN";
  case CachedExtent::extent_state_t::EXIST_MUTATION_PENDING:
    return out << "EXIST_MUTATION_PENDING";
  case CachedExtent::extent_state_t::INVALID:
    return out << "INVALID";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext)
{
  return ext.print(out);
}

CachedExtent::~CachedExtent()
{
  if (parent_index) {
    assert(is_linked());
    parent_index->erase(*this);
  }
}

std::ostream &LogicalCachedExtent::print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr;
  if (pin) {
    out << ", pin=" << *pin;
  } else {
    out << ", pin=empty";
  }
  return print_detail_l(out);
}

std::ostream &operator<<(std::ostream &out, const LBAPin &rhs)
{
  out << "LBAPin(" << rhs.get_key() << "~" << rhs.get_length()
     << "->" << rhs.get_val();
  if (!rhs.get_parent_tracker())
    out << ", parent_tracker=0x0";
  else
    out << ", parent_tracker=" << *rhs.get_parent_tracker();
  return rhs.dump_parent_tracker_trans_views(out);
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

CachedExtentRef ChildNodeTracker::get_child(
  Transaction &t) const
{
  if (!child_per_trans) {
    return child;
  }
  auto it = child_per_trans->find(
    t.get_trans_id(),
    per_trans_view_t::trans_view_compare_t());
  ceph_assert(child != nullptr);
  if (it == child_per_trans->end()) {
    return child;
  } else {
    ceph_assert(parent);
    ceph_assert(!parent->is_pending()
      || (parent->is_pending() && !child_per_trans));
    return (CachedExtent*)(&(*it));
  }
}

void ChildNodeTracker::add_child_per_trans(CachedExtent* c)
{
  ceph_assert(c != nullptr);
  ceph_assert(c->get_mutated_by());
  if (!child_per_trans) {
    child_per_trans = std::make_unique<
      per_trans_view_t::trans_view_set_t>();
  }
  child_per_trans->insert(*c);
}

void ChildNodeTracker::on_transaction_commit(Transaction &t) {
  if (!child_per_trans || child_per_trans->empty()) {
    ceph_assert(child);
    return;
  }
  //TODO: use intrusive set's s_iterator_to
  auto it = child_per_trans->find(
    t.get_trans_id(),
    per_trans_view_t::trans_view_compare_t());
  ceph_assert(it != child_per_trans->end());
  child = (CachedExtent*)(&(*it));
  child_per_trans->erase(it);
}

ChildNodeTracker::ChildNodeTracker(
  ChildNodeTracker &other,
  CachedExtent* parent,
  Transaction &t)
  : parent(parent)
{
  // this constructor should only be called in the following four scenarios:
  // 1. fixed-kv nodes get duplicated for mutation
  // 2. fixed-kv nodes get splitted
  // 3. fixed-kv nodes get merged
  // 4. fixed-kv nodes get balanced
  //
  // so the new tracker must be within the view of a specific transaction
  ceph_assert(parent && other.parent);
  ceph_assert(parent != other.parent); // should only happens when allocating
				       // new fixed kv nodes
  if (other.is_empty())
    return;
  auto e = other.get_child(t);
  // point the new tracker to its child
  update_child(e.get());
  if (e->is_pending_by_me(t.get_trans_id())) {
    // if the child is in the current transaction's view, there should be
    // no relation between the child and its original parent after the
    // construction of this new child tracker.

    // the child points to the new tracker
    assert(e.get() != other.get_child_global_view()
      || other.is_parent_mutated_by_me(t.get_trans_id()));
    if (is_lba_node(e->get_type())) {
      e->cast<lba_manager::btree::LBANode>()->parent_tracker = this;
    } else if (is_backref_node(e->get_type())) {
      e->cast<backref::BackrefNode>()->parent_tracker = this;
    } else {
      ceph_assert(e->is_logical());
      auto l_e = e->cast<LogicalCachedExtent>();
      auto &pin = l_e->get_pin();
      pin.new_parent_tracker(this);
    }

    // break the tie between the child and its original parent
    if (e.get() == other.get_child_global_view()) {
      assert(e->get_mutated_by() == other.get_parent_mutated_by());
      assert(!e->transaction_view_hook.is_linked());
      other.remove_child(e.get());
    } else {
      assert(!other.is_parent_mutated_by_me(t.get_trans_id()));
      assert(other.get_parent_mutated_by() == 0);
      assert(e->transaction_view_hook.is_linked());
      e->transaction_view_hook.unlink();
    }
  } else {
    if (is_lba_node(e->get_type())) {
      auto lba_node = e->cast<lba_manager::btree::LBANode>();
      auto [iter, inserted] = lba_node->parent_tracker_trans_views.insert(*this);
      if (!inserted)
	lba_node->parent_tracker_trans_views.replace_node(iter, *this);
    } else if (is_backref_node(e->get_type())) {
      auto backref_node = e->cast<backref::BackrefNode>();
      auto [iter, inserted] = backref_node->parent_tracker_trans_views.insert(*this);
      if (!inserted)
	backref_node->parent_tracker_trans_views.replace_node(iter, *this);
    } else {
      ceph_assert(e->is_logical());
      auto l_e = e->cast<LogicalCachedExtent>();
      auto &pin = l_e->get_pin();
      pin.new_parent_tracker_trans_view(this);
    }
  }
}

ChildNodeTracker::~ChildNodeTracker()
{
  logger().debug("{}: killing myself: {}", __func__, (void*)this);
  if (child) {
    if (is_lba_node(child->get_type())) {
      auto lba_node = child->cast<lba_manager::btree::LBANode>();
      if (lba_node->parent_tracker == this)
	lba_node->parent_tracker = nullptr;
    } else if (is_backref_node(child->get_type())) {
      auto backref_node = child->cast<backref::BackrefNode>();
      if (backref_node->parent_tracker == this)
	backref_node->parent_tracker = nullptr;
    } else {
      ceph_assert(child->is_logical());
      auto l_e = child->cast<LogicalCachedExtent>();
      auto &pin = l_e->get_pin();
      pin.remove_parent_tracker(this);
    }
  }
}

CachedExtentRef LogicalCachedExtent::duplicate_for_write(Transaction &t) {
  auto ext = get_mutable_duplication(t);
  ext->mutated_by = t.get_trans_id();
  auto &ptracker = pin->get_parent_tracker(t.get_trans_id());
  if (ptracker.is_parent_mutated_by_me(t.get_trans_id())) {
    ptracker.update_child(ext.get());
  } else {
    ptracker.add_child_per_trans(ext.get());
  }
  return ext;
}

void LogicalCachedExtent::on_replace_extent(Transaction &t, CachedExtent& prev) {
  assert(pin);
  assert(pin->get_parent_tracker_trans_views().empty());
  auto &ptracker = pin->get_parent_tracker(t.get_trans_id());
  ptracker.on_transaction_commit(t);
  auto logical_prev = prev.cast<LogicalCachedExtent>();
  auto &trans_views = logical_prev->get_pin().get_parent_tracker_trans_views();
  for (auto &ptracker : trans_views) {
    ptracker.update_child(this);
  }
}

std::ostream &operator<<(std::ostream &out, const ChildNodeTracker &rhs) {
  return out << "child_node_tracker(addr=" << (void*)&rhs
	     << ", child=" << (void*)rhs.child
	     << ", parent=" << (void*)rhs.parent
	     << ", trans_views: "<< ((!rhs.child_per_trans)
				     ? 0 :rhs.child_per_trans->size())
	     << ")";
}

}
