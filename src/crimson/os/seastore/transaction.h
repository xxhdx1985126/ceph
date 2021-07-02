// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>
#include "crimson/common/log.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

struct retired_extent_gate_t;
class SeaStore;
class Transaction;

class ExtentRewriter;

/**
 * Transaction
 *
 * Representation of in-progress mutation. Used exclusively through Cache methods.
 */
class Transaction {
public:
  using Ref = std::unique_ptr<Transaction>;
  enum class get_extent_ret {
    PRESENT,
    ABSENT,
    RETIRED
  };
  get_extent_ret get_extent(paddr_t addr, CachedExtentRef *out) {
    ::crimson::get_logger(ceph_subsys_seastore).debug("{} {}", __func__, addr);
    if (retired_set.count(addr)) {
      return get_extent_ret::RETIRED;
    } else if (auto iter = find_in_write_set(addr);
	iter != write_set.end()) {
      ::crimson::get_logger(ceph_subsys_seastore).debug("{} found in write_set {}", __func__, *iter);
      if (out)
	*out = CachedExtentRef(&*iter);
      return get_extent_ret::PRESENT;
    } else if (
      auto iter = read_set.find(addr);
      iter != read_set.end()) {
      ::crimson::get_logger(ceph_subsys_seastore).debug("{} found in read_set {}", __func__, *iter->ref);
      if (out)
	*out = iter->ref;
      return get_extent_ret::PRESENT;
    } else {
      return get_extent_ret::ABSENT;
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    if (!ref->is_initial_pending()) {
      // && retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.insert(ref);
    } else {
      ref->state = CachedExtent::extent_state_t::INVALID;
    }
    if (ref->is_pending()) {
      write_set.erase(*ref);
    }
  }

  void add_to_retired_uncached(paddr_t addr, extent_len_t length) {
    retired_uncached.emplace_back(std::make_pair(addr, length));
  }

  void dump_write_set() {
    for (auto it = write_set.begin();
	 it != write_set.end();
	 ++it) {
	::crimson::get_logger(ceph_subsys_seastore).debug(
	    "{} write_set: {}", (void*)this, *it);
    }
  }
  void dump_write_set(paddr_t addr) {
    for (auto it = write_set.begin();
	 it != write_set.end();
	 ++it) {
	if (it->poffset == addr) {
	  ::crimson::get_logger(ceph_subsys_seastore).debug(
	      "{} write_set: {}", (void*)this, *it);
	}
    }
  }

  void add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) return;

    auto [iter, inserted] = read_set.emplace(this, ref);
    if (!inserted) {
      ::crimson::get_logger(ceph_subsys_seastore).debug(
	"{} {} {} already in read_set",
	__func__,
	(void*)this,
	*ref);
    }
  }

  //XXX: boost::intrusive::set seems to have a bug in it, in which
  //	 it may leak the first item when trying to find a specific item
  CachedExtent::index::iterator find_in_write_set(paddr_t addr) {
    auto it = write_set.begin();
    for (; it != write_set.end(); ++it) {
	if (it->poffset == addr) {
	  break;
	}
    }
    return it;
  }

  void add_fresh_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    fresh_block_list.push_back(ref);
    ref->set_paddr(make_record_relative_paddr(offset));
    offset += ref->get_length();
    write_set.insert(*ref);
  }

  void add_rewrite_extent(CachedExtentRef ref) {
    assert(ref->is_logical());
    auto lref = ref->cast<LogicalCachedExtent>();
    rewrite_block_list[lref->extent_writer].emplace_back(lref);
    write_set.insert(*ref);
  }

  void add_mutated_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    mutated_block_list.push_back(ref);
    write_set.insert(*ref);
  }

  void mark_segment_to_release(segment_id_t segment) {
    assert(to_release == NULL_SEG_ID);
    to_release = segment;
  }

  segment_id_t get_segment_to_release() const {
    return to_release;
  }

  const auto &get_fresh_block_list() {
    return fresh_block_list;
  }

  const auto &get_mutated_block_list() {
    return mutated_block_list;
  }

  auto &get_rewrite_block_list() {
    return rewrite_block_list;
  }

  const auto &get_retired_set() {
    return retired_set;
  }

  bool is_weak() const {
    return weak;
  }

  bool is_conflicted() const {
    return conflicted;
  }

  auto &get_handle() {
    return handle;
  }

  Transaction(
    OrderingHandle &&handle,
    bool weak,
    journal_seq_t initiated_after
  ) : weak(weak),
      retired_gate_token(initiated_after),
      handle(std::move(handle))
  {}


  ~Transaction() {
    for (auto i = write_set.begin();
	 i != write_set.end();) {
      i->state = CachedExtent::extent_state_t::INVALID;
      write_set.erase(*i++);
    }
  }

  friend class crimson::os::seastore::SeaStore;
  friend class TransactionConflictCondition;

  void reset_preserve_handle(journal_seq_t initiated_after) {
    root.reset();
    offset = 0;
    read_set.clear();
    write_set.clear();
    fresh_block_list.clear();
    mutated_block_list.clear();
    retired_set.clear();
    to_release = NULL_SEG_ID;
    retired_uncached.clear();
    retired_gate_token.reset(initiated_after);
    conflicted = false;
  }

private:
  friend class Cache;
  friend Ref make_test_transaction();

  /**
   * If set, *this may not be used to perform writes and will not provide
   * consistentency allowing operations using to avoid maintaining a read_set.
   */
  const bool weak;

  RootBlockRef root;        ///< ref to root if read or written by transaction

  segment_off_t offset = 0; ///< relative offset of next block

  read_set_t<Transaction> read_set; ///< set of extents read by paddr
  ExtentIndex write_set;            ///< set of extents written by paddr

  std::list<CachedExtentRef> fresh_block_list;   ///< list of fresh blocks
  std::list<CachedExtentRef> mutated_block_list; ///< list of mutated blocks
  std::map<ExtentRewriter*, std::list<LogicalCachedExtentRef>>
    rewrite_block_list; ///< list of rewriten blocks

  pextent_set_t retired_set; ///< list of extents mutated by this transaction

  ///< if != NULL_SEG_ID, release this segment after completion
  segment_id_t to_release = NULL_SEG_ID;

  std::vector<std::pair<paddr_t, extent_len_t>> retired_uncached;

  retired_extent_gate_t::token_t retired_gate_token;

  bool conflicted = false;

  OrderingHandle handle;
};
using TransactionRef = Transaction::Ref;

/// Should only be used with dummy staged-fltree node extent manager
inline TransactionRef make_test_transaction() {
  return std::make_unique<Transaction>(
    get_dummy_ordering_handle(),
    false,
    journal_seq_t{}
  );
}

struct TransactionConflictCondition {
  class transaction_conflict final : public std::exception {
  public:
    const char* what() const noexcept final {
      return "transaction conflict detected";
    }
  };

public:
  TransactionConflictCondition(Transaction &t) : t(t) {}

  template <typename Fut>
  std::pair<bool, std::optional<Fut>> may_interrupt() {
    if (t.conflicted) {
      return {
	true,
	seastar::futurize<Fut>::make_exception_future(
	  transaction_conflict())};
    } else {
      return {false, std::optional<Fut>()};
    }
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, transaction_conflict>;


  static bool is_interruption(std::exception_ptr& eptr) {
    return *eptr.__cxa_exception_type() == typeid(transaction_conflict);
  }

private:
  Transaction &t;
};

using trans_intr = crimson::interruptible::interruptor<
  TransactionConflictCondition
  >;

template <typename E>
using trans_iertr =
  crimson::interruptible::interruptible_errorator<
    TransactionConflictCondition,
    E
  >;

template <typename F, typename... Args>
auto with_trans_intr(Transaction &t, F &&f, Args&&... args) {
  return trans_intr::with_interruption_to_error<crimson::ct_error::eagain>(
    std::move(f),
    TransactionConflictCondition(t),
    t,
    std::forward<Args>(args)...);
}

template <typename T>
using with_trans_ertr = typename T::base_ertr::template extend<crimson::ct_error::eagain>;

}
