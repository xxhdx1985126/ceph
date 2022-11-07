// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/btree/fixed_kv_node.h"

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
CachedExtent* CachedExtent::get_transactional_view(Transaction &t) {
  return get_transactional_view(t.get_trans_id());
}

CachedExtent* CachedExtent::get_transactional_view(transaction_id_t tid) {
  auto it = mutation_pendings.find(tid, trans_spec_view_t::cmp_t());
  if (it != mutation_pendings.end()) {
    return (CachedExtent*)&(*it);
  } else {
    return this;
  }
}

void child_pos_t::link_child(ChildableCachedExtent *c) {
  get_parent<FixedKVNode<laddr_t>>()->link_child(c, pos);
  child = c;
}

std::ostream &LogicalCachedExtent::print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr
      << ", parent_tracker=" << (void*)parent_tracker.get();
  if (parent_tracker) {
    out << ", parent=" << (void*)parent_tracker->get_parent().get();
  }
  return print_detail_l(out);
}

LogicalCachedExtent::~LogicalCachedExtent() {
  if (parent_tracker && is_valid() && !is_pending()) {
    assert(parent_tracker->get_parent());
    auto parent = parent_tracker->get_parent<FixedKVNode<laddr_t>>();
    auto off = parent->lower_bound_offset(laddr);
    assert(parent->get_key_from_idx(off) == laddr);
    assert(parent->stable_children[off] == this);
    parent->stable_children[off] = nullptr;
  }
}

void LogicalCachedExtent::on_replace_prior(Transaction &t) {
  assert(is_mutation_pending());
  auto &prior = (LogicalCachedExtent&)(*get_prior_instance());
  parent_tracker = prior.parent_tracker;
  assert(parent_tracker->get_parent());
  auto parent = parent_tracker->get_parent<FixedKVNode<laddr_t>>();
  //TODO: can this search be avoided?
  auto off = parent->lower_bound_offset(laddr);
  assert(parent->get_key_from_idx(off) == laddr);
  parent->stable_children[off] = this;
}

parent_tracker_t::~parent_tracker_t() {
  // this is parent's tracker, reset it
  auto &p = (FixedKVNode<laddr_t>&)*parent;
  if (p.my_tracker == this) {
   p.my_tracker = nullptr;
  }
}

std::ostream &operator<<(std::ostream &out, const LBAPin &rhs)
{
  return out << "LBAPin(" << rhs.get_key() << "~" << rhs.get_length()
	     << "->" << rhs.get_val();
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

}
