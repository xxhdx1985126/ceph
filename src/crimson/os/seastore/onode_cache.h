// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "seastar/core/metrics_registration.hh"

namespace crimson::os::seastore {

class BackgroundListener;
class TransactionManager;
class ExtentPlacementManager;

class OnodeCache {
public:
  OnodeCache();
  ~OnodeCache();

  // onode cache
  void touch(laddr_t laddr);
  void add(laddr_t laddr);
  void remove(laddr_t laddr);
  void remove_direct(laddr_t laddr);
  bool should_evict() const;
  std::size_t get_evict_size_per_cycle() const {
    return evict_size_per_cycle;
  }

  // pending extents
  void add_extent(CachedExtentRef extent);
  void remove_extent(CachedExtentRef extent);
  bool should_write_out_extents() const {
    return read_size >= cached_extents_size_limit;
  }
  std::list<CachedExtentRef> get_cached_extents(Transaction &t);
  std::size_t get_cached_extents_size() {
    return read_size;
  }
  void reset_cached_extents();

  // helper method
  void set_epm(ExtentPlacementManager *e) {
    epm = e;
  }
  void set_background_callback(BackgroundListener *b) {
    background_callback = b;
  }
  void set_transaction_manager(TransactionManager *tm_) {
    tm = tm_;
  }

  // background process
  seastar::future<> write_cache();
  seastar::future<> evict();

  // evict state
  struct evict_state_t {
    std::list<CachedExtentRef> cold_extents;
    std::vector<laddr_t> cold_onode;
    std::vector<laddr_t>::iterator cold_onode_cursor;
    std::vector<laddr_t> completed;
    lba_pin_list_t pins;
    std::size_t processed_pin_size;
    std::size_t write_size;
    friend std::ostream& operator<<(std::ostream& out, const evict_state_t& s) {
      return out << "evict_state_t(cold_extents_length=" << s.cold_extents.size()
		 // << ", cold_onodes_length=" << s.cold_onode.size()
		 << ", current_cold_onodes=" << *s.cold_onode_cursor
		 << ", completed_size=" << s.completed.size()
		 << ", write_size=" << s.write_size
		 << ", current_pins_length=" << s.pins.size()
		 << ", processed_pin_size=" << s.processed_pin_size << ")";
    }
    void reset() {
      cold_extents.clear();
      cold_onode.clear();
      cold_onode_cursor = cold_onode.end();
      completed.clear();
      pins.clear();
      processed_pin_size = 0;
      write_size = 0;
    }
  } evict_state;

private:
  auto find_laddr(laddr_t laddr);
  laddr_t get_base_laddr(laddr_t laddr) {
    return laddr - laddr % onode_length_and_alignment;
  }

  struct entry_t : public boost::intrusive_ref_counter<
    entry_t, boost::thread_unsafe_counter> {
    explicit entry_t(laddr_t laddr) : laddr(laddr) {}
    laddr_t laddr;

    boost::intrusive::list_member_hook<> list_hook;
    boost::intrusive::set_member_hook<> set_hook;

    bool is_in_lru() const {
      return list_hook.is_linked();
    }

    friend auto operator<=>(const entry_t& l, const entry_t& r) {
      return l.laddr <=> r.laddr;
    }
    struct entry_key_t {
      using type = laddr_t;
      const type& operator()(const entry_t& e) const {
        return e.laddr;
      }
    };
  };
  using entry_ref_t = boost::intrusive_ptr<entry_t>;
  using laddr_list_t = boost::intrusive::list<entry_t,
    boost::intrusive::member_hook<
      entry_t, boost::intrusive::list_member_hook<>, &entry_t::list_hook>>;
  using laddr_set_t = boost::intrusive::set<entry_t,
    boost::intrusive::key_of_value<entry_t::entry_key_t>,
    boost::intrusive::member_hook<
      entry_t, boost::intrusive::set_member_hook<>, &entry_t::set_hook>>;

  entry_ref_t create_entry(laddr_t laddr);
  void release_entry(entry_ref_t e) {
    remove_from_lru(*e);
    laddr_set.erase(laddr_set.s_iterator_to(*e));
    intrusive_ptr_release(&*e);
    stats.onode_counts--;
  }

  void move_to_top(entry_t &e) {
    remove_from_lru(e);
    lru.push_back(e);
  }
  void remove_from_lru(entry_t &e) {
    if (e.is_in_lru()) {
      lru.erase(lru.s_iterator_to(e));
    }
  }

  // other components
  BackgroundListener *background_callback;
  ExtentPlacementManager *epm;
  TransactionManager *tm;

  // onode cache
  laddr_set_t laddr_set;
  laddr_list_t lru;

  // cached extents
  CachedExtent::read_list read_cache;
  CachedExtent::read_list pending_write;
  std::set<paddr_t> read_cache_paddr;
  uint64_t read_size;

  // config
  std::size_t onode_length_and_alignment;
  std::size_t onode_cache_memory_capacity;
  std::size_t evict_size_per_cycle;
  std::size_t cached_extents_size_limit;

  // metrics
  struct {
    std::size_t object_data_block_counts;
    std::size_t onode_counts;
    std::size_t read_counts;
    std::size_t read_size;
    std::size_t evicted_counts;
    std::size_t evicted_size;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();
};
using OnodeCacheRef = std::unique_ptr<OnodeCache>;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::OnodeCache::evict_state_t> : fmt::ostream_formatter {};
#endif
