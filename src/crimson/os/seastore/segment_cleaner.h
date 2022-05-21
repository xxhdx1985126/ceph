// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>
#include <seastar/core/metrics_types.hh>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager_group.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/segment_seq_allocator.h"

namespace crimson::os::seastore {

/*
 * segment_info_t
 *
 * Maintains the tracked information for a segment.
 * It is read-only outside segments_info_t.
 */
struct segment_info_t {
  using time_point = seastar::lowres_system_clock::time_point;

  // segment_info_t is initiated as set_empty()
  Segment::segment_state_t state = Segment::segment_state_t::EMPTY;

  // Will be non-null for any segments in the current journal
  segment_seq_t seq = NULL_SEG_SEQ;

  segment_type_t type = segment_type_t::NULL_SEG;

  time_point last_modified;
  time_point last_rewritten;

  std::size_t written_to = 0;

  bool is_in_journal(journal_seq_t tail_committed) const {
    return type == segment_type_t::JOURNAL &&
           tail_committed.segment_seq <= seq;
  }

  bool is_empty() const {
    return state == Segment::segment_state_t::EMPTY;
  }

  bool is_closed() const {
    return state == Segment::segment_state_t::CLOSED;
  }

  bool is_open() const {
    return state == Segment::segment_state_t::OPEN;
  }

  void init_closed(segment_seq_t, segment_type_t, std::size_t);

  void set_open(segment_seq_t, segment_type_t);

  void set_empty();

  void set_closed();

  void update_last_modified_rewritten(
      time_point _last_modified, time_point _last_rewritten) {
    if (_last_modified != time_point() && last_modified < _last_modified) {
      last_modified = _last_modified;
    }
    if (_last_rewritten != time_point() && last_rewritten < _last_rewritten) {
      last_rewritten = _last_rewritten;
    }
  }
};

std::ostream& operator<<(std::ostream&, const segment_info_t&);

/*
 * segments_info_t
 *
 * Keep track of all segments and related information.
 */
class segments_info_t {
public:
  using time_point = seastar::lowres_system_clock::time_point;

  segments_info_t() {
    reset();
  }

  const segment_info_t& operator[](segment_id_t id) const {
    return segments[id];
  }

  auto begin() const {
    return segments.begin();
  }

  auto end() const {
    return segments.end();
  }

  std::size_t get_num_segments() const {
    assert(segments.size() > 0);
    return segments.size();
  }
  std::size_t get_segment_size() const {
    assert(segment_size > 0);
    return segment_size;
  }
  std::size_t get_num_in_journal_open() const {
    return num_in_journal_open;
  }
  std::size_t get_num_type_journal() const {
    return num_type_journal;
  }
  std::size_t get_num_type_ool() const {
    return num_type_ool;
  }
  std::size_t get_num_open() const {
    return num_open;
  }
  std::size_t get_num_empty() const {
    return num_empty;
  }
  std::size_t get_num_closed() const {
    return num_closed;
  }
  std::size_t get_count_open_journal() const {
    return count_open_journal;
  }
  std::size_t get_count_open_ool() const {
    return count_open_ool;
  }
  std::size_t get_count_release_journal() const {
    return count_release_journal;
  }
  std::size_t get_count_release_ool() const {
    return count_release_ool;
  }
  std::size_t get_count_close_journal() const {
    return count_close_journal;
  }
  std::size_t get_count_close_ool() const {
    return count_close_ool;
  }

  std::size_t get_total_bytes() const {
    return total_bytes;
  }
  /// the available space that is writable, including in open segments
  std::size_t get_available_bytes() const {
    return num_empty * get_segment_size() + avail_bytes_in_open;
  }
  /// the unavailable space that is not writable
  std::size_t get_unavailable_bytes() const {
    assert(total_bytes >= get_available_bytes());
    return total_bytes - get_available_bytes();
  }
  std::size_t get_available_bytes_in_open() const {
    return avail_bytes_in_open;
  }
  double get_available_ratio() const {
    return (double)get_available_bytes() / (double)total_bytes;
  }

  journal_seq_t get_journal_head() const {
    if (unlikely(journal_segment_id == NULL_SEG_ID)) {
      return JOURNAL_SEQ_NULL;
    }
    auto &segment_info = segments[journal_segment_id];
    assert(!segment_info.is_empty());
    assert(segment_info.type == segment_type_t::JOURNAL);
    assert(segment_info.seq != NULL_SEG_SEQ);
    return journal_seq_t{
      segment_info.seq,
      paddr_t::make_seg_paddr(
        journal_segment_id,
        segment_info.written_to)
    };
  }

  void reset();

  void add_segment_manager(SegmentManager &segment_manager);

  // initiate non-empty segments, the others are by default empty
  void init_closed(segment_id_t, segment_seq_t, segment_type_t);

  void mark_open(segment_id_t, segment_seq_t, segment_type_t);

  void mark_empty(segment_id_t);

  void mark_closed(segment_id_t);

  void update_written_to(segment_type_t, paddr_t);

  void update_last_modified_rewritten(
      segment_id_t id, time_point last_modified, time_point last_rewritten) {
    segments[id].update_last_modified_rewritten(last_modified, last_rewritten);
  }

private:
  // See reset() for member initialization
  segment_map_t<segment_info_t> segments;

  std::size_t segment_size;

  segment_id_t journal_segment_id;
  std::size_t num_in_journal_open;
  std::size_t num_type_journal;
  std::size_t num_type_ool;

  std::size_t num_open;
  std::size_t num_empty;
  std::size_t num_closed;

  std::size_t count_open_journal;
  std::size_t count_open_ool;
  std::size_t count_release_journal;
  std::size_t count_release_ool;
  std::size_t count_close_journal;
  std::size_t count_close_ool;

  std::size_t total_bytes;
  std::size_t avail_bytes_in_open;
};

/**
 * Callback interface for managing available segments
 */
class SegmentProvider {
public:
  virtual journal_seq_t get_journal_tail_target() const = 0;

  virtual const segment_info_t& get_seg_info(segment_id_t id) const = 0;

  virtual segment_id_t allocate_segment(
      segment_seq_t seq, segment_type_t type) = 0;

  virtual journal_seq_t get_dirty_extents_replay_from() const = 0;

  virtual journal_seq_t get_alloc_info_replay_from() const = 0;

  virtual void close_segment(segment_id_t) = 0;

  virtual void update_journal_tail_committed(journal_seq_t tail_committed) = 0;

  virtual void update_segment_avail_bytes(segment_type_t, paddr_t) = 0;

  virtual SegmentManagerGroup* get_segment_manager_group() = 0;

  virtual ~SegmentProvider() {}
};

class SpaceTrackerI {
public:
  virtual int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t get_usage(
    segment_id_t segment) const = 0;

  virtual bool equals(const SpaceTrackerI &other) const = 0;

  virtual std::unique_ptr<SpaceTrackerI> make_empty() const = 0;

  virtual void dump_usage(segment_id_t) const = 0;

  virtual double calc_utilization(segment_id_t segment) const = 0;

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class SpaceTrackerSimple : public SpaceTrackerI {
  struct segment_bytes_t {
    int64_t live_bytes = 0;
    seastore_off_t total_bytes = 0;
  };
  // Tracks live space for each segment
  segment_map_t<segment_bytes_t> live_bytes_by_segment;

  int64_t update_usage(segment_id_t segment, int64_t delta) {
    live_bytes_by_segment[segment].live_bytes += delta;
    assert(live_bytes_by_segment[segment].live_bytes >= 0);
    return live_bytes_by_segment[segment].live_bytes;
  }
public:
  SpaceTrackerSimple(const SpaceTrackerSimple &) = default;
  SpaceTrackerSimple(const std::vector<SegmentManager*> &sms) {
    for (auto sm : sms) {
      live_bytes_by_segment.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	{0, sm->get_segment_size()});
    }
  }

  int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, len);
  }

  int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, -(int64_t)len);
  }

  int64_t get_usage(segment_id_t segment) const final {
    return live_bytes_by_segment[segment].live_bytes;
  }

  double calc_utilization(segment_id_t segment) const final {
    auto& seg_bytes = live_bytes_by_segment[segment];
    return (double)seg_bytes.live_bytes / (double)seg_bytes.total_bytes;
  }

  void dump_usage(segment_id_t) const final;

  void reset() final {
    for (auto &i : live_bytes_by_segment) {
      i.second = {0, 0};
    }
  }

  SpaceTrackerIRef make_empty() const final {
    auto ret = SpaceTrackerIRef(new SpaceTrackerSimple(*this));
    ret->reset();
    return ret;
  }

  bool equals(const SpaceTrackerI &other) const;
};

class SpaceTrackerDetailed : public SpaceTrackerI {
  class SegmentMap {
    int64_t used = 0;
    seastore_off_t total_bytes = 0;
    std::vector<bool> bitmap;

  public:
    SegmentMap(
      size_t blocks,
      seastore_off_t total_bytes)
    : total_bytes(total_bytes),
      bitmap(blocks, false) {}

    int64_t update_usage(int64_t delta) {
      used += delta;
      return used;
    }

    int64_t allocate(
      device_segment_id_t segment,
      seastore_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t release(
      device_segment_id_t segment,
      seastore_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t get_usage() const {
      return used;
    }

    void dump_usage(extent_len_t block_size) const;

    double calc_utilization() const {
      return (double)used / (double)total_bytes;
    }

    void reset() {
      used = 0;
      for (auto &&i: bitmap) {
	i = false;
      }
    }
  };

  // Tracks live space for each segment
  segment_map_t<SegmentMap> segment_usage;
  std::vector<size_t> block_size_by_segment_manager;

public:
  SpaceTrackerDetailed(const SpaceTrackerDetailed &) = default;
  SpaceTrackerDetailed(const std::vector<SegmentManager*> &sms)
  {
    block_size_by_segment_manager.resize(DEVICE_ID_MAX, 0);
    for (auto sm : sms) {
      segment_usage.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	SegmentMap(
	  sm->get_segment_size() / sm->get_block_size(),
	  sm->get_segment_size()));
      block_size_by_segment_manager[sm->get_device_id()] = sm->get_block_size();
    }
  }

  int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return segment_usage[segment].allocate(
      segment.device_segment_id(),
      offset,
      len,
      block_size_by_segment_manager[segment.device_id()]);
  }

  int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return segment_usage[segment].release(
      segment.device_segment_id(),
      offset,
      len,
      block_size_by_segment_manager[segment.device_id()]);
  }

  int64_t get_usage(segment_id_t segment) const final {
    return segment_usage[segment].get_usage();
  }

  double calc_utilization(segment_id_t segment) const final {
    return segment_usage[segment].calc_utilization();
  }

  void dump_usage(segment_id_t seg) const final;

  void reset() final {
    for (auto &i: segment_usage) {
      i.second.reset();
    }
  }

  SpaceTrackerIRef make_empty() const final {
    auto ret = SpaceTrackerIRef(new SpaceTrackerDetailed(*this));
    ret->reset();
    return ret;
  }

  bool equals(const SpaceTrackerI &other) const;
};


class SegmentCleaner : public SegmentProvider {
public:
  using time_point = seastar::lowres_system_clock::time_point;
  using duration = seastar::lowres_system_clock::duration;

  /// Config
  struct config_t {
    /// Number of minimum journal segments to stop trimming.
    size_t target_journal_segments = 0;
    /// Number of maximum journal segments to block user transactions.
    size_t max_journal_segments = 0;

    /// Number of journal segments the transactions in which can
    /// have their corresponding backrefs unmerged
    size_t target_backref_inflight_segments = 0;

    /// Ratio of maximum available space to disable reclaiming.
    double available_ratio_gc_max = 0;
    /// Ratio of minimum available space to force reclaiming.
    double available_ratio_hard_limit = 0;

    /// Ratio of maximum reclaimable space to block user transactions.
    double reclaim_ratio_hard_limit = 0;
    /// Ratio of minimum reclaimable space to stop reclaiming.
    double reclaim_ratio_gc_threshold = 0;

    /// Number of bytes to reclaim per cycle
    size_t reclaim_bytes_per_cycle = 0;

    /// Number of bytes to rewrite dirty per cycle
    size_t rewrite_dirty_bytes_per_cycle = 0;

    /// Number of bytes to rewrite backref per cycle
    size_t rewrite_backref_bytes_per_cycle = 0;

    void validate() const {
      ceph_assert(max_journal_segments > target_journal_segments);
      ceph_assert(available_ratio_gc_max > available_ratio_hard_limit);
      ceph_assert(reclaim_ratio_hard_limit > reclaim_ratio_gc_threshold);
      ceph_assert(reclaim_bytes_per_cycle > 0);
      ceph_assert(rewrite_dirty_bytes_per_cycle > 0);
      ceph_assert(rewrite_backref_bytes_per_cycle > 0);
    }

    static config_t get_default() {
      return config_t{
	  12,   // target_journal_segments
	  16,   // max_journal_segments
	  2,	// target_backref_inflight_segments
	  .9,   // available_ratio_gc_max
	  .2,   // available_ratio_hard_limit
	  .8,   // reclaim_ratio_hard_limit
	  .6,   // reclaim_ratio_gc_threshold
	  1<<20,// reclaim_bytes_per_cycle
	  1<<17,// rewrite_dirty_bytes_per_cycle
	  1<<24 // rewrite_backref_bytes_per_cycle
	};
    }

    static config_t get_test() {
      return config_t{
	  2,    // target_journal_segments
	  4,    // max_journal_segments
	  2,	// target_backref_inflight_segments
	  .9,   // available_ratio_gc_max
	  .2,   // available_ratio_hard_limit
	  .8,   // reclaim_ratio_hard_limit
	  .6,   // reclaim_ratio_gc_threshold
	  1<<20,// reclaim_bytes_per_cycle
	  1<<17,// rewrite_dirty_bytes_per_cycle
	  1<<24 // rewrite_backref_bytes_per_cycle
	};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;

    virtual TransactionRef create_transaction(
        Transaction::src_t, const char*) = 0;

    /// Creates empty transaction with interruptible context
    template <typename Func>
    auto with_transaction_intr(
        Transaction::src_t src,
        const char* name,
        Func &&f) {
      return seastar::do_with(
        create_transaction(src, name),
        [f=std::forward<Func>(f)](auto &ref_t) mutable {
          return with_trans_intr(
            *ref_t,
            [f=std::forward<Func>(f)](auto& t) mutable {
              return f(t);
            }
          );
        }
      );
    }

    /// See Cache::get_next_dirty_extents
    using get_next_dirty_extents_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
      Transaction &t,     ///< [in] current transaction
      journal_seq_t bound,///< [in] return extents with dirty_from < bound
      size_t max_bytes    ///< [in] return up to max_bytes of extents
    ) = 0;

    using extent_mapping_ertr = crimson::errorator<
      crimson::ct_error::input_output_error,
      crimson::ct_error::eagain>;
    using extent_mapping_iertr = trans_iertr<
      crimson::errorator<
	crimson::ct_error::input_output_error>
      >;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  extent may be invalid, implementation must correctly
     * handle finding the current instance if it is still alive and
     * otherwise ignore it.
     */
    using rewrite_extent_iertr = extent_mapping_iertr;
    using rewrite_extent_ret = rewrite_extent_iertr::future<>;
    virtual rewrite_extent_ret rewrite_extent(
      Transaction &t,
      CachedExtentRef extent) = 0;

    /**
     * get_extent_if_live
     *
     * Returns extent at specified location if still referenced by
     * lba_manager and not removed by t.
     *
     * See TransactionManager::get_extent_if_live and
     * LBAManager::get_physical_extent_if_live.
     */
    using get_extent_if_live_iertr = extent_mapping_iertr;
    using get_extent_if_live_ret = get_extent_if_live_iertr::future<
      CachedExtentRef>;
    virtual get_extent_if_live_ret get_extent_if_live(
      Transaction &t,
      extent_types_t type,
      paddr_t addr,
      laddr_t laddr,
      seastore_off_t len) = 0;

    /**
     * submit_transaction_direct
     *
     * Submits transaction without any space throttling.
     */
    using submit_transaction_direct_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using submit_transaction_direct_ret =
      submit_transaction_direct_iertr::future<>;
    virtual submit_transaction_direct_ret submit_transaction_direct(
      Transaction &t,
      std::optional<journal_seq_t> seq_to_trim = std::nullopt) = 0;
  };

private:
  const bool detailed;
  const config_t config;

  SegmentManagerGroupRef sm_group;
  BackrefManager &backref_manager;
  Cache &cache;

  SpaceTrackerIRef space_tracker;
  segments_info_t segments;
  bool init_complete = false;

  struct {
    /**
     * used_bytes
     *
     * Bytes occupied by live extents
     */
    uint64_t used_bytes = 0;

    /**
     * projected_used_bytes
     *
     * Sum of projected bytes used by each transaction between throttle
     * acquisition and commit completion.  See reserve_projected_usage()
     */
    uint64_t projected_used_bytes = 0;
    uint64_t projected_count = 0;
    uint64_t projected_used_bytes_sum = 0;

    uint64_t closed_journal_used_bytes = 0;
    uint64_t closed_journal_total_bytes = 0;
    uint64_t closed_ool_used_bytes = 0;
    uint64_t closed_ool_total_bytes = 0;

    uint64_t io_blocking_num = 0;
    uint64_t io_count = 0;
    uint64_t io_blocked_count = 0;
    uint64_t io_blocked_count_trim = 0;
    uint64_t io_blocked_count_reclaim = 0;
    uint64_t io_blocked_sum = 0;

    uint64_t reclaiming_bytes = 0;
    uint64_t reclaimed_bytes = 0;
    uint64_t reclaimed_segment_bytes = 0;

    seastar::metrics::histogram segment_util;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  /// target journal_tail for next fresh segment
  journal_seq_t journal_tail_target;

  /// target replay_from for dirty extents
  journal_seq_t dirty_extents_replay_from;

  /// target replay_from for alloc infos
  journal_seq_t alloc_info_replay_from;

  /// most recently committed journal_tail
  journal_seq_t journal_tail_committed;

  ExtentCallbackInterface *ecb = nullptr;

  /// populated if there is an IO blocked on hard limits
  std::optional<seastar::promise<>> blocked_io_wake;

  SegmentSeqAllocatorRef ool_segment_seq_allocator;

  /**
   * disable_trim
   *
   * added to enable unit testing of CircularBoundedJournal before
   * proper support is added to SegmentCleaner.
   * Should be removed once proper support is added. TODO
   */
  bool disable_trim = false;
public:
  SegmentCleaner(
    config_t config,
    SegmentManagerGroupRef&& sm_group,
    BackrefManager &backref_manager,
    Cache &cache,
    bool detailed = false);

  SegmentSeqAllocator& get_ool_segment_seq_allocator() {
    return *ool_segment_seq_allocator;
  }

  using mount_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mount_ret = mount_ertr::future<>;
  mount_ret mount();

  /*
   * SegmentProvider interfaces
   */
  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  const segment_info_t& get_seg_info(segment_id_t id) const final {
    return segments[id];
  }

  segment_id_t allocate_segment(
      segment_seq_t seq, segment_type_t type) final;

  void close_segment(segment_id_t segment) final;

  void update_journal_tail_committed(journal_seq_t committed) final;

  void update_segment_avail_bytes(segment_type_t type, paddr_t offset) final {
    segments.update_written_to(type, offset);
    gc_process.maybe_wake_on_space_used();
  }

  SegmentManagerGroup* get_segment_manager_group() final {
    return sm_group.get();
  }

  journal_seq_t get_dirty_extents_replay_from() const final {
    return dirty_extents_replay_from;
  }

  journal_seq_t get_alloc_info_replay_from() const final {
    return alloc_info_replay_from;
  }

  void update_journal_tail_target(
    journal_seq_t dirty_replay_from,
    journal_seq_t alloc_replay_from);

  void update_alloc_info_replay_from(
    journal_seq_t alloc_replay_from);

  void init_mkfs() {
    auto journal_head = segments.get_journal_head();
    ceph_assert(disable_trim || journal_head != JOURNAL_SEQ_NULL);
    journal_tail_target = journal_head;
    journal_tail_committed = journal_head;
  }

  using release_ertr = SegmentManagerGroup::release_ertr;
  release_ertr::future<> maybe_release_segment(Transaction &t);

  void adjust_segment_util(double old_usage, double new_usage) {
    auto old_index = get_bucket_index(old_usage);
    auto new_index = get_bucket_index(new_usage);
    assert(stats.segment_util.buckets[old_index].count > 0);
    stats.segment_util.buckets[old_index].count--;
    stats.segment_util.buckets[new_index].count++;
  }

  void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    time_point last_modified = time_point(),
    time_point last_rewritten = time_point(),
    bool init_scan = false) {
    if (addr.get_addr_type() != addr_types_t::SEGMENT)
      return;
    auto& seg_addr = addr.as_seg_paddr();

    if (!init_scan && !init_complete)
      return;

    stats.used_bytes += len;
    auto old_usage = calc_utilization(seg_addr.get_segment_id());
    [[maybe_unused]] auto ret = space_tracker->allocate(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
    auto new_usage = calc_utilization(seg_addr.get_segment_id());
    adjust_segment_util(old_usage, new_usage);

    // use the last extent's last modified time for the calculation of the projected
    // time the segments' live extents are to stay unmodified; this is an approximation
    // of the sprite lfs' segment "age".

    segments.update_last_modified_rewritten(
        seg_addr.get_segment_id(), last_modified, last_rewritten);

    gc_process.maybe_wake_on_space_used();
    assert(ret > 0);
    crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
      "{} segment {} new len: {}~{}, live_bytes: {}",
      __func__,
      seg_addr.get_segment_id(),
      addr,
      len,
      space_tracker->get_usage(seg_addr.get_segment_id()));
  }

  void mark_space_free(
    paddr_t addr,
    extent_len_t len,
    const bool force = false) {
    if (!init_complete && !force)
      return;
    if (addr.get_addr_type() != addr_types_t::SEGMENT)
      return;

    ceph_assert(stats.used_bytes >= len);
    stats.used_bytes -= len;
    auto& seg_addr = addr.as_seg_paddr();

    crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
      "{} segment {} free len: {}~{}",
      __func__,
      seg_addr.get_segment_id(),
      addr,
      len);
    auto old_usage = calc_utilization(seg_addr.get_segment_id());
    [[maybe_unused]] auto ret = space_tracker->release(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
    auto new_usage = calc_utilization(seg_addr.get_segment_id());
    adjust_segment_util(old_usage, new_usage);
    maybe_wake_gc_blocked_io();
    assert(ret >= 0);
    crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
      "{} segment {} free len: {}~{}, live_bytes: {}",
      __func__,
      seg_addr.get_segment_id(),
      addr,
      len,
      space_tracker->get_usage(seg_addr.get_segment_id()));
  }

  SpaceTrackerIRef get_empty_space_tracker() const {
    return space_tracker->make_empty();
  }

  void complete_init();

  store_statfs_t stat() const {
    store_statfs_t st;
    st.total = segments.get_total_bytes();
    st.available = segments.get_total_bytes() - stats.used_bytes;
    st.allocated = stats.used_bytes;
    st.data_stored = stats.used_bytes;

    // TODO add per extent type counters for omap_allocated and
    // internal metadata
    return st;
  }

  seastar::future<> stop() {
    return gc_process.stop();
  }

  seastar::future<> run_until_halt() {
    return gc_process.run_until_halt();
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  bool debug_check_space(const SpaceTrackerI &tracker) {
    return space_tracker->equals(tracker);
  }

  void set_disable_trim(bool val) {
    disable_trim = val;
  }

  using work_ertr = ExtentCallbackInterface::extent_mapping_ertr;
  using work_iertr = ExtentCallbackInterface::extent_mapping_iertr;

private:
  /*
   * 10 buckets for the number of closed segments by usage
   * 2 extra buckets for the number of open and empty segments
   */
  static constexpr double UTIL_STATE_OPEN = 1.05;
  static constexpr double UTIL_STATE_EMPTY = 1.15;
  static constexpr std::size_t UTIL_BUCKETS = 12;
  static std::size_t get_bucket_index(double util) {
    auto index = std::floor(util * 10);
    assert(index < UTIL_BUCKETS);
    return index;
  }
  double calc_utilization(segment_id_t id) const {
    auto& info = segments[id];
    if (info.is_open()) {
      return UTIL_STATE_OPEN;
    } else if (info.is_empty()) {
      return UTIL_STATE_EMPTY;
    } else {
      auto ret = space_tracker->calc_utilization(id);
      assert(ret >= 0 && ret < 1);
      return ret;
    }
  }

  // journal status helpers

  double calc_gc_benefit_cost(segment_id_t id) const {
    double util = calc_utilization(id);
    ceph_assert(util >= 0 && util < 1);
    auto cur_time = seastar::lowres_system_clock::now();
    auto segment = segments[id];
    assert(cur_time >= segment.last_modified);
    auto segment_age =
      cur_time - std::max(segment.last_modified, segment.last_rewritten);
    uint64_t age = segment_age.count();
    return (1 - util) * age / (1 + util);
  }

  journal_seq_t get_next_gc_target() const {
    segment_id_t id = NULL_SEG_ID;
    segment_seq_t seq = NULL_SEG_SEQ;
    double max_benefit_cost = 0;
    for (auto it = segments.begin();
	 it != segments.end();
	 ++it) {
      auto _id = it->first;
      const auto& segment_info = it->second;
      if (segment_info.is_closed() &&
	  !segment_info.is_in_journal(journal_tail_committed)) {
        double benefit_cost = calc_gc_benefit_cost(_id);
        if (benefit_cost > max_benefit_cost) {
          id = _id;
          seq = segment_info.seq;
          max_benefit_cost = benefit_cost;
        }
      }
    }
    if (id != NULL_SEG_ID) {
      crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
	"SegmentCleaner::get_next_gc_target: segment {} seq {}, benefit_cost {}",
	id,
	seq,
	max_benefit_cost);
      return journal_seq_t{seq, paddr_t::make_seg_paddr(id, 0)};
    } else {
      ceph_assert(get_segments_reclaimable() == 0);
      // see gc_should_reclaim_space()
      ceph_abort("impossible!");
      return JOURNAL_SEQ_NULL;
    }
  }

  /**
   * rewrite_dirty
   *
   * Writes out dirty blocks dirtied earlier than limit.
   */
  using rewrite_dirty_iertr = work_iertr;
  using rewrite_dirty_ret = rewrite_dirty_iertr::future<>;
  rewrite_dirty_ret rewrite_dirty(
    Transaction &t,
    journal_seq_t limit);

  using trim_backrefs_iertr = work_iertr;
  using trim_backrefs_ret = trim_backrefs_iertr::future<journal_seq_t>;
  trim_backrefs_ret trim_backrefs(
    Transaction &t,
    journal_seq_t limit);

  journal_seq_t get_dirty_tail() const {
    auto ret = segments.get_journal_head();
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.target_journal_segments) {
      ret.segment_seq -= config.target_journal_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }

  journal_seq_t get_dirty_tail_limit() const {
    auto ret = segments.get_journal_head();
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.max_journal_segments) {
      ret.segment_seq -= config.max_journal_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }

  journal_seq_t get_backref_tail() const {
    auto ret = segments.get_journal_head();
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.target_backref_inflight_segments) {
      ret.segment_seq -= config.target_backref_inflight_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }
  // GC status helpers
  std::optional<paddr_t> next_reclaim_pos;

  bool final_reclaim() {
    return next_reclaim_pos->as_seg_paddr().get_segment_off()
      + config.reclaim_bytes_per_cycle >= (size_t)segments.get_segment_size();
  }

  /**
   * GCProcess
   *
   * Background gc process.
   */
  using gc_cycle_ret = seastar::future<>;
  class GCProcess {
    std::optional<gc_cycle_ret> process_join;

    SegmentCleaner &cleaner;

    std::optional<seastar::promise<>> blocking;

    bool is_stopping() const {
      return !process_join;
    }

    gc_cycle_ret run();

    void wake() {
      if (blocking) {
	blocking->set_value();
	blocking = std::nullopt;
      }
    }

    seastar::future<> maybe_wait_should_run() {
      return seastar::do_until(
	[this] {
	  cleaner.log_gc_state("GCProcess::maybe_wait_should_run");
	  return is_stopping() || cleaner.gc_should_run();
	},
	[this] {
	  ceph_assert(!blocking);
	  blocking = seastar::promise<>();
	  return blocking->get_future();
	});
    }
  public:
    GCProcess(SegmentCleaner &cleaner) : cleaner(cleaner) {}

    void start() {
      ceph_assert(is_stopping());
      process_join = seastar::now(); // allow run()
      process_join = run();
      assert(!is_stopping());
    }

    gc_cycle_ret stop() {
      if (is_stopping()) {
        return seastar::now();
      }
      auto ret = std::move(*process_join);
      process_join.reset();
      assert(is_stopping());
      wake();
      return ret;
    }

    gc_cycle_ret run_until_halt() {
      ceph_assert(is_stopping());
      return seastar::do_until(
	[this] {
	  cleaner.log_gc_state("GCProcess::run_until_halt");
	  return !cleaner.gc_should_run();
	},
	[this] {
	  return cleaner.do_gc_cycle();
	});
    }

    void maybe_wake_on_space_used() {
      if (is_stopping()) {
        return;
      }
      if (cleaner.gc_should_run()) {
	wake();
      }
    }
  } gc_process;

  using gc_ertr = work_ertr::extend_ertr<
    SegmentManagerGroup::scan_extents_ertr
    >;

  gc_cycle_ret do_gc_cycle();

  using gc_trim_journal_ertr = gc_ertr;
  using gc_trim_journal_ret = gc_trim_journal_ertr::future<>;
  gc_trim_journal_ret gc_trim_journal();

  using gc_trim_backref_ertr = gc_ertr;
  using gc_trim_backref_ret = gc_trim_backref_ertr::future<journal_seq_t>;
  gc_trim_backref_ret gc_trim_backref(journal_seq_t limit);

  using gc_reclaim_space_ertr = gc_ertr;
  using gc_reclaim_space_ret = gc_reclaim_space_ertr::future<>;
  gc_reclaim_space_ret gc_reclaim_space();

  using retrieve_backref_extents_iertr = work_iertr;
  using retrieve_backref_extents_ret =
    retrieve_backref_extents_iertr::future<>;
  retrieve_backref_extents_ret _retrieve_backref_extents(
    Transaction &t,
    std::set<
      Cache::backref_extent_buf_entry_t,
      Cache::backref_extent_buf_entry_t::cmp_t> &&backref_extents,
    std::vector<CachedExtentRef> &extents);

  using retrieve_live_extents_iertr = work_iertr;
  using retrieve_live_extents_ret =
    retrieve_live_extents_iertr::future<journal_seq_t>;
  retrieve_live_extents_ret _retrieve_live_extents(
    Transaction &t,
    std::set<
      backref_buf_entry_t,
      backref_buf_entry_t::cmp_t> &&backrefs,
    std::vector<CachedExtentRef> &extents);

  /*
   * Segments calculations
   */
  std::size_t get_segments_in_journal() const {
    if (!init_complete) {
      return 0;
    }
    if (journal_tail_committed == JOURNAL_SEQ_NULL) {
      return segments.get_num_type_journal();
    }
    auto journal_head = segments.get_journal_head();
    assert(journal_head != JOURNAL_SEQ_NULL);
    assert(journal_head.segment_seq >= journal_tail_committed.segment_seq);
    return journal_head.segment_seq + 1 - journal_tail_committed.segment_seq;
  }
  std::size_t get_segments_in_journal_closed() const {
    auto in_journal = get_segments_in_journal();
    auto in_journal_open = segments.get_num_in_journal_open();
    if (in_journal >= in_journal_open) {
      return in_journal - in_journal_open;
    } else {
      return 0;
    }
  }
  std::size_t get_segments_reclaimable() const {
    assert(segments.get_num_closed() >= get_segments_in_journal_closed());
    return segments.get_num_closed() - get_segments_in_journal_closed();
  }

  /*
   * Space calculations
   */
  /// the unavailable space that is not reclaimable yet
  std::size_t get_unavailable_unreclaimable_bytes() const {
    auto ret = (segments.get_num_open() + get_segments_in_journal_closed()) *
               segments.get_segment_size();
    assert(ret >= segments.get_available_bytes_in_open());
    return ret - segments.get_available_bytes_in_open();
  }
  /// the unavailable space that can be reclaimed
  std::size_t get_unavailable_reclaimable_bytes() const {
    auto ret = get_segments_reclaimable() * segments.get_segment_size();
    ceph_assert(ret + get_unavailable_unreclaimable_bytes() == segments.get_unavailable_bytes());
    return ret;
  }
  /// the unavailable space that is not alive
  std::size_t get_unavailable_unused_bytes() const {
    assert(segments.get_unavailable_bytes() > stats.used_bytes);
    return segments.get_unavailable_bytes() - stats.used_bytes;
  }
  double get_reclaim_ratio() const {
    if (segments.get_unavailable_bytes() == 0) return 0;
    return (double)get_unavailable_unused_bytes() / (double)segments.get_unavailable_bytes();
  }

  /*
   * Space calculations (projected)
   */
  std::size_t get_projected_available_bytes() const {
    return (segments.get_available_bytes() > stats.projected_used_bytes) ?
      segments.get_available_bytes() - stats.projected_used_bytes:
      0;
  }
  double get_projected_available_ratio() const {
    return (double)get_projected_available_bytes() /
      (double)segments.get_total_bytes();
  }

  /*
   * Journal sizes
   */
  std::size_t get_dirty_journal_size() const {
    auto journal_head = segments.get_journal_head();
    if (journal_head == JOURNAL_SEQ_NULL ||
        dirty_extents_replay_from == JOURNAL_SEQ_NULL) {
      return 0;
    }
    return (journal_head.segment_seq - dirty_extents_replay_from.segment_seq) *
           segments.get_segment_size() +
           journal_head.offset.as_seg_paddr().get_segment_off() -
           segments.get_segment_size() -
           dirty_extents_replay_from.offset.as_seg_paddr().get_segment_off();
  }

  std::size_t get_alloc_journal_size() const {
    auto journal_head = segments.get_journal_head();
    if (journal_head == JOURNAL_SEQ_NULL ||
        alloc_info_replay_from == JOURNAL_SEQ_NULL) {
      return 0;
    }
    return (journal_head.segment_seq - alloc_info_replay_from.segment_seq) *
           segments.get_segment_size() +
           journal_head.offset.as_seg_paddr().get_segment_off() -
           segments.get_segment_size() -
           alloc_info_replay_from.offset.as_seg_paddr().get_segment_off();
  }

  /**
   * should_block_on_gc
   *
   * Encapsulates whether block pending gc.
   */
  bool should_block_on_trim() const {
    if (disable_trim) return false;
    return get_dirty_tail_limit() > journal_tail_target;
  }

  bool should_block_on_reclaim() const {
    if (disable_trim) return false;
    if (get_segments_reclaimable() == 0) {
      return false;
    }
    auto aratio = get_projected_available_ratio();
    auto rratio = get_reclaim_ratio();
    return (
      (aratio < config.available_ratio_hard_limit) ||
      ((aratio < config.available_ratio_gc_max) &&
       (rratio > config.reclaim_ratio_hard_limit))
    );
  }

  bool should_block_on_gc() const {
    return should_block_on_trim() || should_block_on_reclaim();
  }

  void log_gc_state(const char *caller) const {
    auto &logger = crimson::get_logger(ceph_subsys_seastore_cleaner);
    if (logger.is_enabled(seastar::log_level::debug) &&
	!disable_trim) {
      logger.debug(
	"SegmentCleaner::log_gc_state({}): "
	"empty {}, "
	"open {}, "
	"closed {}, "
	"in_journal {}, "
	"total {}B, "
	"available {}B, "
	"unavailable {}B, "
	"unavailable_used {}B, "
	"unavailable_unused {}B; "
	"reclaim_ratio {}, "
	"available_ratio {}, "
	"should_block_on_gc {}, "
	"gc_should_reclaim_space {}, "
	"journal_head {}, "
	"journal_tail_target {}, "
	"journal_tail_commit {}, "
	"dirty_tail {}, "
	"dirty_tail_limit {}, "
	"gc_should_trim_journal {}, ",
	caller,
	segments.get_num_empty(),
	segments.get_num_open(),
	segments.get_num_closed(),
	get_segments_in_journal(),
	segments.get_total_bytes(),
	segments.get_available_bytes(),
	segments.get_unavailable_bytes(),
	stats.used_bytes,
	get_unavailable_unused_bytes(),
	get_reclaim_ratio(),
	segments.get_available_ratio(),
	should_block_on_gc(),
	gc_should_reclaim_space(),
	segments.get_journal_head(),
	journal_tail_target,
	journal_tail_committed,
	get_dirty_tail(),
	get_dirty_tail_limit(),
	gc_should_trim_journal()
      );
    }
  }

public:
  seastar::future<> reserve_projected_usage(size_t projected_usage) {
    if (disable_trim) {
      return seastar::now();
    }
    ceph_assert(init_complete);
    // The pipeline configuration prevents another IO from entering
    // prepare until the prior one exits and clears this.
    ceph_assert(!blocked_io_wake);
    ++stats.io_count;
    bool is_blocked = false;
    if (should_block_on_trim()) {
      is_blocked = true;
      ++stats.io_blocked_count_trim;
    }
    if (should_block_on_reclaim()) {
      is_blocked = true;
      ++stats.io_blocked_count_reclaim;
    }
    if (is_blocked) {
      ++stats.io_blocking_num;
      ++stats.io_blocked_count;
      stats.io_blocked_sum += stats.io_blocking_num;
    }
    return seastar::do_until(
      [this] {
	log_gc_state("await_hard_limits");
	return !should_block_on_gc();
      },
      [this] {
	blocked_io_wake = seastar::promise<>();
	return blocked_io_wake->get_future();
      }
    ).then([this, projected_usage, is_blocked] {
      ceph_assert(!blocked_io_wake);
      stats.projected_used_bytes += projected_usage;
      ++stats.projected_count;
      stats.projected_used_bytes_sum += stats.projected_used_bytes;
      if (is_blocked) {
        assert(stats.io_blocking_num > 0);
        --stats.io_blocking_num;
      }
    });
  }

  void release_projected_usage(size_t projected_usage) {
    if (disable_trim) return;
    ceph_assert(init_complete);
    ceph_assert(stats.projected_used_bytes >= projected_usage);
    stats.projected_used_bytes -= projected_usage;
    return maybe_wake_gc_blocked_io();
  }
private:
  void maybe_wake_gc_blocked_io() {
    if (!init_complete) {
      return;
    }
    if (!should_block_on_gc() && blocked_io_wake) {
      blocked_io_wake->set_value();
      blocked_io_wake = std::nullopt;
    }
  }

  using scan_extents_ret_bare =
    std::vector<std::pair<segment_id_t, segment_header_t>>;
  using scan_extents_ertr = SegmentManagerGroup::scan_extents_ertr;
  using scan_extents_ret = scan_extents_ertr::future<>;
  scan_extents_ret scan_nonfull_segment(
    const segment_header_t& header,
    scan_extents_ret_bare& segment_set,
    segment_id_t segment_id);

  /**
   * gc_should_reclaim_space
   *
   * Encapsulates logic for whether gc should be reclaiming segment space.
   */
  bool gc_should_reclaim_space() const {
    if (disable_trim) return false;
    if (get_segments_reclaimable() == 0) {
      return false;
    }
    auto aratio = segments.get_available_ratio();
    auto rratio = get_reclaim_ratio();
    return (
      (aratio < config.available_ratio_hard_limit) ||
      ((aratio < config.available_ratio_gc_max) &&
       (rratio > config.reclaim_ratio_gc_threshold))
    );
  }

  /**
   * gc_should_trim_journal
   *
   * Encapsulates logic for whether gc should be reclaiming segment space.
   */
  bool gc_should_trim_journal() const {
    return get_dirty_tail() > journal_tail_target;
  }

  bool gc_should_trim_backref() const {
    return get_backref_tail() > alloc_info_replay_from;
  }
  /**
   * gc_should_run
   *
   * True if gc should be running.
   */
  bool gc_should_run() const {
    if (disable_trim) return false;
    ceph_assert(init_complete);
    return gc_should_reclaim_space()
      || gc_should_trim_journal()
      || gc_should_trim_backref();
  }

  void init_mark_segment_closed(
      segment_id_t segment,
      segment_seq_t seq,
      segment_type_t s_type) {
    ceph_assert(!init_complete);
    auto old_usage = calc_utilization(segment);
    segments.init_closed(segment, seq, s_type);
    auto new_usage = calc_utilization(segment);
    adjust_segment_util(old_usage, new_usage);
    if (s_type == segment_type_t::OOL) {
      ool_segment_seq_allocator->set_next_segment_seq(seq);
    }
  }
};
using SegmentCleanerRef = std::unique_ptr<SegmentCleaner>;

}
