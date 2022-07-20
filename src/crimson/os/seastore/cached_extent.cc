// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/transaction.h"

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

CachedExtentRef ChildNodeTracker::get_child(
  Transaction &t,
  CachedExtent* parent)
{
  if (!child_per_trans) {
    return child;
  }
  auto it = child_per_trans->find(t.get_trans_id());
  ceph_assert(child);
  if (it == child_per_trans->end()) {
    return child;
  } else {
    ceph_assert(!parent->is_pending()
      || (parent->is_pending() && !child_per_trans));
    return it->second;
  }
}

void ChildNodeTracker::add_child_per_trans(
  Transaction &t,
  CachedExtentRef &child)
{
  ceph_assert(child);
  if (!child_per_trans) {
    child_per_trans = std::make_optional<
      std::map<transaction_id_t, CachedExtentRef>>();
  }
  child_per_trans->emplace(t.get_trans_id(), child);
}

void ChildNodeTracker::on_transaction_commit(Transaction &t) {
  ceph_assert(child_per_trans);
  auto it = child_per_trans->find(t.get_trans_id());
  ceph_assert(it != child_per_trans->end());
  child = it->second;
  child_per_trans->erase(it);
}

}
