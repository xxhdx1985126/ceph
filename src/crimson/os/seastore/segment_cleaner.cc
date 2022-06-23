// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/metrics.hh>

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/transaction_manager.h"

SET_SUBSYS(seastore_cleaner);

namespace crimson::os::seastore {

void segment_info_t::set_open(
    segment_seq_t _seq, segment_type_t _type)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  state = Segment::segment_state_t::OPEN;
  seq = _seq;
  type = _type;
  written_to = 0;
}

void segment_info_t::set_empty()
{
  state = Segment::segment_state_t::EMPTY;
  seq = NULL_SEG_SEQ;
  type = segment_type_t::NULL_SEG;
  last_modified = {};
  last_rewritten = {};
  written_to = 0;
}

void segment_info_t::set_closed()
{
  state = Segment::segment_state_t::CLOSED;
  // the rest of information is unchanged
}

void segment_info_t::init_closed(
    segment_seq_t _seq, segment_type_t _type, std::size_t seg_size)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  state = Segment::segment_state_t::CLOSED;
  seq = _seq;
  type = _type;
  written_to = seg_size;
}

std::ostream& operator<<(std::ostream &out, const segment_info_t &info)
{
  out << "seg_info_t("
      << "state=" << info.state;
  if (info.is_empty()) {
    // pass
  } else { // open or closed
    out << ", seq=" << segment_seq_printer_t{info.seq}
        << ", type=" << info.type
        << ", last_modified=" << info.last_modified.time_since_epoch()
        << ", last_rewritten=" << info.last_rewritten.time_since_epoch()
        << ", written_to=" << info.written_to;
  }
  return out << ")";
}

void segments_info_t::reset()
{
  segments.clear();

  segment_size = 0;

  journal_segment_id = NULL_SEG_ID;
  num_in_journal_open = 0;
  num_type_journal = 0;
  num_type_ool = 0;

  num_open = 0;
  num_empty = 0;
  num_closed = 0;

  count_open_journal = 0;
  count_open_ool = 0;
  count_release_journal = 0;
  count_release_ool = 0;
  count_close_journal = 0;
  count_close_ool = 0;

  total_bytes = 0;
  avail_bytes_in_open = 0;
}

void segments_info_t::add_segment_manager(
    SegmentManager &segment_manager)
{
  LOG_PREFIX(segments_info_t::add_segment_manager);
  device_id_t d_id = segment_manager.get_device_id();
  auto ssize = segment_manager.get_segment_size();
  auto nsegments = segment_manager.get_num_segments();
  auto sm_size = segment_manager.get_size();
  INFO("adding segment manager {}, size={}, ssize={}, segments={}",
       device_id_printer_t{d_id}, sm_size, ssize, nsegments);
  ceph_assert(ssize > 0);
  ceph_assert(nsegments > 0);
  ceph_assert(sm_size > 0);

  // also validate if the device is duplicated
  segments.add_device(d_id, nsegments, segment_info_t{});

  // assume all the segment managers share the same settings as follows.
  if (segment_size == 0) {
    ceph_assert(ssize > 0);
    segment_size = ssize;
  } else {
    ceph_assert(segment_size == (std::size_t)ssize);
  }

  // NOTE: by default the segments are empty
  num_empty += nsegments;

  total_bytes += sm_size;
}

void segments_info_t::init_closed(
    segment_id_t segment, segment_seq_t seq, segment_type_t type)
{
  LOG_PREFIX(segments_info_t::init_closed);
  auto& segment_info = segments[segment];
  INFO("initiating {} {} {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_seq_printer_t{seq}, type,
       segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  segment_info.init_closed(seq, type, get_segment_size());
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_closed;
  if (type == segment_type_t::JOURNAL) {
    // init_closed won't initialize journal_segment_id
    ceph_assert(get_journal_head() == JOURNAL_SEQ_NULL);
    ++num_type_journal;
  } else {
    ++num_type_ool;
  }
  // do not increment count_close_*;
}

void segments_info_t::mark_open(
    segment_id_t segment, segment_seq_t seq, segment_type_t type)
{
  LOG_PREFIX(segments_info_t::mark_open);
  auto& segment_info = segments[segment];
  INFO("opening {} {} {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_seq_printer_t{seq}, type,
       segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  segment_info.set_open(seq, type);
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_open;
  if (type == segment_type_t::JOURNAL) {
    if (journal_segment_id != NULL_SEG_ID) {
      auto& last_journal_segment = segments[journal_segment_id];
      ceph_assert(last_journal_segment.is_closed());
      ceph_assert(last_journal_segment.type == segment_type_t::JOURNAL);
      ceph_assert(last_journal_segment.seq + 1 == seq);
    }
    journal_segment_id = segment;

    ++num_in_journal_open;
    ++num_type_journal;
    ++count_open_journal;
  } else {
    ++num_type_ool;
    ++count_open_ool;
  }
  ceph_assert(segment_info.written_to == 0);
  avail_bytes_in_open += get_segment_size();
}

void segments_info_t::mark_empty(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_empty);
  auto& segment_info = segments[segment];
  INFO("releasing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_closed());
  auto type = segment_info.type;
  assert(type != segment_type_t::NULL_SEG);
  segment_info.set_empty();
  ceph_assert(num_closed > 0);
  --num_closed;
  ++num_empty;
  if (type == segment_type_t::JOURNAL) {
    ceph_assert(num_type_journal > 0);
    --num_type_journal;
    ++count_release_journal;
  } else {
    ceph_assert(num_type_ool > 0);
    --num_type_ool;
    ++count_release_ool;
  }
}

void segments_info_t::mark_closed(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_closed);
  auto& segment_info = segments[segment];
  INFO("closing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_open());
  segment_info.set_closed();
  ceph_assert(num_open > 0);
  --num_open;
  ++num_closed;
  if (segment_info.type == segment_type_t::JOURNAL) {
    ceph_assert(num_in_journal_open > 0);
    --num_in_journal_open;
    ++count_close_journal;
  } else {
    ++count_close_ool;
  }
  ceph_assert(get_segment_size() >= segment_info.written_to);
  auto seg_avail_bytes = get_segment_size() - segment_info.written_to;
  ceph_assert(avail_bytes_in_open >= seg_avail_bytes);
  avail_bytes_in_open -= seg_avail_bytes;
}

void segments_info_t::update_written_to(
    segment_type_t type,
    paddr_t offset)
{
  LOG_PREFIX(segments_info_t::update_written_to);
  auto& saddr = offset.as_seg_paddr();
  auto& segment_info = segments[saddr.get_segment_id()];
  if (!segment_info.is_open()) {
    ERROR("segment is not open, not updating, type={}, offset={}, {}",
          type, offset, segment_info);
    ceph_abort();
  }

  auto new_written_to = static_cast<std::size_t>(saddr.get_segment_off());
  ceph_assert(new_written_to <= get_segment_size());
  if (segment_info.written_to > new_written_to) {
    ERROR("written_to should not decrease! type={}, offset={}, {}",
          type, offset, segment_info);
    ceph_abort();
  }

  DEBUG("type={}, offset={}, {}", type, offset, segment_info);
  ceph_assert(type == segment_info.type);
  auto avail_deduction = new_written_to - segment_info.written_to;
  ceph_assert(avail_bytes_in_open >= avail_deduction);
  avail_bytes_in_open -= avail_deduction;
  segment_info.written_to = new_written_to;
}

bool SpaceTrackerSimple::equals(const SpaceTrackerI &_other) const
{
  LOG_PREFIX(SpaceTrackerSimple::equals);
  const auto &other = static_cast<const SpaceTrackerSimple&>(_other);

  if (other.live_bytes_by_segment.size() != live_bytes_by_segment.size()) {
    ERROR("different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = live_bytes_by_segment.begin(), j = other.live_bytes_by_segment.begin();
       i != live_bytes_by_segment.end(); ++i, ++j) {
    if (i->second.live_bytes != j->second.live_bytes) {
      all_match = false;
      DEBUG("segment_id {} live bytes mismatch *this: {}, other: {}",
            i->first, i->second.live_bytes, j->second.live_bytes);
    }
  }
  return all_match;
}

int64_t SpaceTrackerDetailed::SegmentMap::allocate(
  device_segment_id_t segment,
  seastore_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  LOG_PREFIX(SegmentMap::allocate);
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (bitmap[i]) {
      if (!error) {
        ERROR("found allocated in {}, {} ~ {}", segment, offset, len);
	error = true;
      }
      DEBUG("block {} allocated", i * block_size);
    }
    bitmap[i] = true;
  }
  return update_usage(len);
}

int64_t SpaceTrackerDetailed::SegmentMap::release(
  device_segment_id_t segment,
  seastore_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  LOG_PREFIX(SegmentMap::release);
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (!bitmap[i]) {
      if (!error) {
	ERROR("found unallocated in {}, {} ~ {}", segment, offset, len);
	error = true;
      }
      DEBUG("block {} unallocated", i * block_size);
    }
    bitmap[i] = false;
  }
  return update_usage(-(int64_t)len);
}

bool SpaceTrackerDetailed::equals(const SpaceTrackerI &_other) const
{
  LOG_PREFIX(SpaceTrackerDetailed::equals);
  const auto &other = static_cast<const SpaceTrackerDetailed&>(_other);

  if (other.segment_usage.size() != segment_usage.size()) {
    ERROR("different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = segment_usage.begin(), j = other.segment_usage.begin();
       i != segment_usage.end(); ++i, ++j) {
    if (i->second.get_usage() != j->second.get_usage()) {
      all_match = false;
      ERROR("segment_id {} live bytes mismatch *this: {}, other: {}",
            i->first, i->second.get_usage(), j->second.get_usage());
    }
  }
  return all_match;
}

void SpaceTrackerDetailed::SegmentMap::dump_usage(extent_len_t block_size) const
{
  LOG_PREFIX(SegmentMap::dump_usage);
  INFO("dump start");
  for (unsigned i = 0; i < bitmap.size(); ++i) {
    if (bitmap[i]) {
      LOCAL_LOGGER.info("    {} still live", i * block_size);
    }
  }
}

void SpaceTrackerDetailed::dump_usage(segment_id_t id) const
{
  LOG_PREFIX(SpaceTrackerDetailed::dump_usage);
  INFO("{}", id);
  segment_usage[id].dump_usage(
    block_size_by_segment_manager[id.device_id()]);
}

void SpaceTrackerSimple::dump_usage(segment_id_t id) const
{
  LOG_PREFIX(SpaceTrackerSimple::dump_usage);
  INFO("id: {}, live_bytes: {}",
       id, live_bytes_by_segment[id].live_bytes);
}

SegmentCleaner::SegmentCleaner(
  config_t config,
  SegmentManagerGroupRef&& sm_group,
  BackrefManager &backref_manager,
  bool detailed)
  : detailed(detailed),
    config(config),
    sm_group(std::move(sm_group)),
    backref_manager(backref_manager),
    ool_segment_seq_allocator(
      new SegmentSeqAllocator(segment_type_t::OOL)),
    gc_process(*this)
{
  config.validate();
}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  stats.segment_util.buckets.resize(UTIL_BUCKETS);
  std::size_t i;
  for (i = 0; i < UTIL_BUCKETS; ++i) {
    stats.segment_util.buckets[i].upper_bound = ((double)(i + 1)) / 10;
    stats.segment_util.buckets[i].count = 0;
  }
  // NOTE: by default the segments are empty
  i = get_bucket_index(UTIL_STATE_EMPTY);
  stats.segment_util.buckets[i].count = segments.get_num_segments();

  metrics.add_group("segment_cleaner", {
    sm::make_counter("segments_number",
		     [this] { return segments.get_num_segments(); },
		     sm::description("the number of segments")),
    sm::make_counter("segment_size",
		     [this] { return segments.get_segment_size(); },
		     sm::description("the bytes of a segment")),
    sm::make_counter("segments_in_journal",
		     [this] { return get_segments_in_journal(); },
		     sm::description("the number of segments in journal")),
    sm::make_counter("segments_type_journal",
		     [this] { return segments.get_num_type_journal(); },
		     sm::description("the number of segments typed journal")),
    sm::make_counter("segments_type_ool",
		     [this] { return segments.get_num_type_ool(); },
		     sm::description("the number of segments typed out-of-line")),
    sm::make_counter("segments_open",
		     [this] { return segments.get_num_open(); },
		     sm::description("the number of open segments")),
    sm::make_counter("segments_empty",
		     [this] { return segments.get_num_empty(); },
		     sm::description("the number of empty segments")),
    sm::make_counter("segments_closed",
		     [this] { return segments.get_num_closed(); },
		     sm::description("the number of closed segments")),

    sm::make_counter("segments_count_open_journal",
		     [this] { return segments.get_count_open_journal(); },
		     sm::description("the count of open journal segment operations")),
    sm::make_counter("segments_count_open_ool",
		     [this] { return segments.get_count_open_ool(); },
		     sm::description("the count of open ool segment operations")),
    sm::make_counter("segments_count_release_journal",
		     [this] { return segments.get_count_release_journal(); },
		     sm::description("the count of release journal segment operations")),
    sm::make_counter("segments_count_release_ool",
		     [this] { return segments.get_count_release_ool(); },
		     sm::description("the count of release ool segment operations")),
    sm::make_counter("segments_count_close_journal",
		     [this] { return segments.get_count_close_journal(); },
		     sm::description("the count of close journal segment operations")),
    sm::make_counter("segments_count_close_ool",
		     [this] { return segments.get_count_close_ool(); },
		     sm::description("the count of close ool segment operations")),

    sm::make_counter("total_bytes",
		     [this] { return segments.get_total_bytes(); },
		     sm::description("the size of the space")),
    sm::make_counter("available_bytes",
		     [this] { return segments.get_available_bytes(); },
		     sm::description("the size of the space is available")),
    sm::make_counter("unavailable_unreclaimable_bytes",
		     [this] { return get_unavailable_unreclaimable_bytes(); },
		     sm::description("the size of the space is unavailable and unreclaimable")),
    sm::make_counter("unavailable_reclaimable_bytes",
		     [this] { return get_unavailable_reclaimable_bytes(); },
		     sm::description("the size of the space is unavailable and reclaimable")),
    sm::make_counter("used_bytes", stats.used_bytes,
		     sm::description("the size of the space occupied by live extents")),
    sm::make_counter("unavailable_unused_bytes",
		     [this] { return get_unavailable_unused_bytes(); },
		     sm::description("the size of the space is unavailable and not alive")),

    sm::make_counter("dirty_journal_bytes",
		     [this] { return get_dirty_journal_size(); },
		     sm::description("the size of the journal for dirty extents")),
    sm::make_counter("alloc_journal_bytes",
		     [this] { return get_alloc_journal_size(); },
		     sm::description("the size of the journal for alloc info")),

    sm::make_counter("projected_count", stats.projected_count,
		    sm::description("the number of projected usage reservations")),
    sm::make_counter("projected_used_bytes_sum", stats.projected_used_bytes_sum,
		    sm::description("the sum of the projected usage in bytes")),

    sm::make_counter("io_count", stats.io_count,
		    sm::description("the sum of IOs")),
    sm::make_counter("io_blocked_count", stats.io_blocked_count,
		    sm::description("IOs that are blocked by gc")),
    sm::make_counter("io_blocked_count_trim", stats.io_blocked_count_trim,
		    sm::description("IOs that are blocked by trimming")),
    sm::make_counter("io_blocked_count_reclaim", stats.io_blocked_count_reclaim,
		    sm::description("IOs that are blocked by reclaimming")),
    sm::make_counter("io_blocked_sum", stats.io_blocked_sum,
		     sm::description("the sum of blocking IOs")),

    sm::make_counter("reclaimed_bytes", stats.reclaimed_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("reclaimed_segment_bytes", stats.reclaimed_segment_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("closed_journal_used_bytes", stats.closed_journal_used_bytes,
		     sm::description("used bytes when close a journal segment")),
    sm::make_counter("closed_journal_total_bytes", stats.closed_journal_total_bytes,
		     sm::description("total bytes of closed journal segments")),
    sm::make_counter("closed_ool_used_bytes", stats.closed_ool_used_bytes,
		     sm::description("used bytes when close a ool segment")),
    sm::make_counter("closed_ool_total_bytes", stats.closed_ool_total_bytes,
		     sm::description("total bytes of closed ool segments")),

    sm::make_gauge("available_ratio",
                   [this] { return segments.get_available_ratio(); },
                   sm::description("ratio of available space to total space")),
    sm::make_gauge("reclaim_ratio",
                   [this] { return get_reclaim_ratio(); },
                   sm::description("ratio of reclaimable space to unavailable space")),

    sm::make_counter("accumulated_blocked_ios", stats.accumulated_blocked_ios,
		     sm::description("accumulated total number of ios that were blocked by gc")),
    sm::make_counter("reclaim_rewrite_bytes", stats.sr_stats.reclaim_rewrite_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("reclaiming_bytes", stats.sr_stats.reclaiming_bytes,
		     sm::description("bytes being reclaimed")),
    sm::make_counter("ios_blocking", stats.ios_blocking,
		     sm::description("IOs that are blocking on space usage")),
    sm::make_counter("projected_used_bytes", stats.projected_used_bytes,
		     sm::description("the size of the space going to be occupied by new extents")),
    sm::make_gauge("accumulated_get_backref_mappings_time",
		   stats.sr_stats.accum_stats.accumulated_get_backref_mappings_time,
		   sm::description("accumulated time spent getting backref"
				   " mappings for space reclamation")),
    sm::make_gauge("accumulated_get_backref_extents_time",
		   stats.sr_stats.accum_stats.accumulated_get_backref_extents_time,
		   sm::description("accumulated time spent getting backref extents"
				   " during space reclamation")),
    sm::make_gauge("accumulated_rewrite_extents_time",
		   stats.sr_stats.accum_stats.accumulated_rewrite_extents_time,
		   sm::description("accumulated time spent rewriting extents"
				   " during space reclamation")),
    sm::make_gauge("accumulated_backref_batch_insert_time",
		   stats.sr_stats.accum_stats.accumulated_backref_batch_insert_time,
		   sm::description("accumulated time spent inserting backrefs to"
				   " the backref tree during space reclamation")),
    sm::make_gauge("accumulated_get_live_extents_time",
		   stats.sr_stats.accum_stats.accumulated_get_live_extents_time,
		   sm::description("accumulated time spent getting live extents"
				   " during space reclamation")),
    sm::make_gauge("accumulated_backrefs_calc_time",
		   stats.sr_stats.accum_stats.accumulated_backrefs_calc_time,
		   sm::description("accumulated time spent calculating backrefs")),
    sm::make_gauge("accumulated_transaction_submit_time",
		   stats.sr_stats.accum_stats.accumulated_transaction_submit_time,
		   sm::description("accumulated time spent committing space reclamation trans")),
    sm::make_gauge("accumulated_reclaim_space_duration",
		   stats.sr_stats.accum_stats.accumulated_reclaim_space_duration,
		   sm::description("accumulated time spent for space reclamation")),
    sm::make_gauge("accumulated_reclaim_space_repeats",
		   stats.sr_stats.accum_stats.accumulated_reclaim_space_repeats,
		   sm::description("accumulated repeats of space reclamation runs")),
    sm::make_gauge("accumulated_time_wasted_on_repeat",
		   stats.sr_stats.accum_stats.accumulated_time_wasted_on_repeat,
		   sm::description("accumulated time wasted on invalidated space reclamation runs")),
    sm::make_gauge("accumulated_gc_backref_trimming_duration",
		   stats.accumulated_gc_backref_trimming_duration,
		   sm::description("accumulated time spent trimming backrefs for gc, journal trimming not included")),
    sm::make_gauge("accumulated_gc_duration",
		   stats.accumulated_gc_duration,
		   sm::description("accumulated time gc takes")),
    sm::make_gauge("accumulated_rewrite_dirty_trans_commit_time",
		   stats.jt_stats.accum_stats.accumulated_rewrite_dirty_trans_commit_time,
		   sm::description("accumulated rewrite dirty transaction commit time")),
    sm::make_gauge("accumulated_prepare_rewrite_dirty_time",
		   stats.jt_stats.accum_stats.accumulated_prepare_rewrite_dirty_time,
		   sm::description("accumulated preparing rewrite dirty trans time")),
    sm::make_gauge("accumulated_io_blocked_rewrite_dirty_trans_commit_time",
		   stats.jt_stats.accum_stats.accumulated_io_blocked_rewrite_dirty_trans_commit_time,
		   sm::description("accumulated rewrite dirty transaction commit time")),
    sm::make_gauge("accumulated_io_blocked_prepare_rewrite_dirty_time",
		   stats.jt_stats.accum_stats.accumulated_io_blocked_prepare_rewrite_dirty_time,
		   sm::description("accumulated preparing rewrite dirty trans time")),
    sm::make_gauge("accumulated_io_blocked_committed_extents",
		   stats.jt_stats.accum_stats.accumulated_io_blocked_committed_extents,
		   sm::description("accumulated committed extents of journal trim trans")),
    sm::make_gauge("gc_backref_trimming_cycles",
		   stats.gc_backref_trimming_cycles,
		   sm::description("cycles of gc_trim_backref runs")),
    sm::make_gauge("accumulated_io_block_time",
		   stats.accumulated_io_block_time,
		   sm::description("accumulated time during which io is blocked")),
    sm::make_counter("reclaim_space_cycles", stats.sr_stats.reclaim_space_cycles,
		     sm::description("cycles for space reclamation")),
    sm::make_counter("accumulated_trim_backrefs_time",
		     stats.jt_stats.accum_stats.accumulated_trim_backrefs_time,
		     sm::description("accumulated time spent trimming backrefs")),
    sm::make_counter("accumulated_rewrite_dirty_time",
		     stats.jt_stats.accum_stats.accumulated_rewrite_dirty_time,
		     sm::description("accumulated time spent rewriting dirty extents")),
    sm::make_counter("accumulated_backrefs_to_be_rewritten",
		     stats.sr_stats.accum_stats.accumulated_backrefs_to_be_rewritten,
		     sm::description("accumulated backref entries corresponding to rewritten extents")),
    sm::make_counter("journal_trim_cycles", stats.jt_stats.cycles,
		     sm::description("cycls of journal trims")),
    sm::make_counter("journal_trim_io_blocked_cycles", stats.jt_stats.cycles_io_blocked,
		     sm::description("cycls of journal trims with io blocked")),
    sm::make_counter("journal_trim_repeats", stats.jt_stats.repeats,
		     sm::description("repeated runs of journal trims")),
    sm::make_counter("journal_trim_duration", stats.jt_stats.duration,
		     sm::description("accumulated duration of journal trims")),
    sm::make_histogram("segment_utilization_distribution",
		       [this]() -> seastar::metrics::histogram& {
		         return stats.segment_util;
		       },
		       sm::description("utilization distribution of all segments"))
  });
}

segment_id_t SegmentCleaner::allocate_segment(
    segment_seq_t seq,
    segment_type_t type)
{
  LOG_PREFIX(SegmentCleaner::allocate_segment);
  assert(seq != NULL_SEG_SEQ);
  for (auto it = segments.begin();
       it != segments.end();
       ++it) {
    auto seg_id = it->first;
    auto& segment_info = it->second;
    if (segment_info.is_empty()) {
      auto old_usage = calc_utilization(seg_id);
      segments.mark_open(seg_id, seq, type);
      auto new_usage = calc_utilization(seg_id);
      adjust_segment_util(old_usage, new_usage);
      INFO("opened, should_block_on_gc {}, projected_avail_ratio {}, "
           "reclaim_ratio {}",
           should_block_on_gc(),
           get_projected_available_ratio(),
           get_reclaim_ratio());
      return seg_id;
    }
  }
  ERROR("out of space with segment_seq={}", segment_seq_printer_t{seq});
  ceph_abort();
  return NULL_SEG_ID;
}

void SegmentCleaner::update_journal_tail_target(
  journal_seq_t dirty_replay_from,
  journal_seq_t alloc_replay_from)
{
  LOG_PREFIX(SegmentCleaner::update_journal_tail_target);
  if (disable_trim) return;
  assert(dirty_replay_from.offset.get_addr_type() != addr_types_t::RANDOM_BLOCK);
  assert(alloc_replay_from.offset.get_addr_type() != addr_types_t::RANDOM_BLOCK);
  if (dirty_extents_replay_from == JOURNAL_SEQ_NULL
      || dirty_replay_from > dirty_extents_replay_from) {
    DEBUG("dirty_extents_replay_from={} => {}",
          dirty_extents_replay_from, dirty_replay_from);
    dirty_extents_replay_from = dirty_replay_from;
  }

  update_alloc_info_replay_from(alloc_replay_from);

  journal_seq_t target = std::min(dirty_replay_from, alloc_replay_from);
  ceph_assert(target != JOURNAL_SEQ_NULL);
  auto journal_head = segments.get_journal_head();
  ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
              journal_head >= target);
  if (journal_tail_target == JOURNAL_SEQ_NULL ||
      target > journal_tail_target) {
    if (!init_complete ||
        journal_tail_target.segment_seq == target.segment_seq) {
      DEBUG("journal_tail_target={} => {}", journal_tail_target, target);
    } else {
      INFO("journal_tail_target={} => {}", journal_tail_target, target);
    }
    journal_tail_target = target;
  }
  gc_process.maybe_wake_on_space_used();
  maybe_wake_gc_blocked_io();
}

void SegmentCleaner::update_alloc_info_replay_from(
  journal_seq_t alloc_replay_from)
{
  LOG_PREFIX(SegmentCleaner::update_alloc_info_replay_from);
  if (alloc_info_replay_from == JOURNAL_SEQ_NULL
      || alloc_replay_from > alloc_info_replay_from) {
    DEBUG("alloc_info_replay_from={} => {}",
          alloc_info_replay_from, alloc_replay_from);
    alloc_info_replay_from = alloc_replay_from;
  }
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  LOG_PREFIX(SegmentCleaner::update_journal_tail_committed);
  assert(committed.offset.get_addr_type() != addr_types_t::RANDOM_BLOCK);
  if (committed == JOURNAL_SEQ_NULL) {
    return;
  }
  auto journal_head = segments.get_journal_head();
  ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
              journal_head >= committed);

  if (journal_tail_committed == JOURNAL_SEQ_NULL ||
      committed > journal_tail_committed) {
    DEBUG("update journal_tail_committed={} => {}",
          journal_tail_committed, committed);
    journal_tail_committed = committed;
  }
  if (journal_tail_target == JOURNAL_SEQ_NULL ||
      committed > journal_tail_target) {
    DEBUG("update journal_tail_target={} => {}",
          journal_tail_target, committed);
    journal_tail_target = committed;
  }
}

void SegmentCleaner::close_segment(segment_id_t segment)
{
  LOG_PREFIX(SegmentCleaner::close_segment);
  auto old_usage = calc_utilization(segment);
  segments.mark_closed(segment);
  auto &seg_info = segments[segment];
  if (seg_info.type == segment_type_t::JOURNAL) {
    stats.closed_journal_used_bytes += space_tracker->get_usage(segment);
    stats.closed_journal_total_bytes += segments.get_segment_size();
  } else {
    stats.closed_ool_used_bytes += space_tracker->get_usage(segment);
    stats.closed_ool_total_bytes += segments.get_segment_size();
  }
  auto new_usage = calc_utilization(segment);
  adjust_segment_util(old_usage, new_usage);
  INFO("closed, should_block_on_gc {}, projected_avail_ratio {}, "
       "reclaim_ratio {}",
       should_block_on_gc(),
       get_projected_available_ratio(),
       get_reclaim_ratio());
}

SegmentCleaner::trim_backrefs_ret SegmentCleaner::trim_backrefs(
  Transaction &t,
  journal_seq_t limit)
{
  return backref_manager.merge_cached_backrefs(
    t,
    limit,
    config.rewrite_backref_bytes_per_cycle
  );
}

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  return ecb->get_next_dirty_extents(
    t,
    limit,
    config.rewrite_dirty_bytes_per_cycle
  ).si_then([=, &t](auto dirty_list) {
    LOG_PREFIX(SegmentCleaner::rewrite_dirty);
    DEBUGT("rewrite {} dirty extents", t, dirty_list.size());
    return seastar::do_with(
      std::move(dirty_list),
      [this, FNAME, &t](auto &dirty_list) {
	return trans_intr::do_for_each(
	  dirty_list,
	  [this, FNAME, &t](auto &e) {
	  DEBUGT("cleaning {}", t, *e);
	  return ecb->rewrite_extent(t, e);
	});
      });
  });
}

SegmentCleaner::gc_cycle_ret SegmentCleaner::GCProcess::run()
{
  return seastar::do_until(
    [this] { return is_stopping(); },
    [this] {
      return maybe_wait_should_run(
      ).then([this] {
	cleaner.log_gc_state("GCProcess::run");

	if (is_stopping()) {
	  return seastar::now();
	} else {
	  auto start = std::chrono::steady_clock::now();
	  return cleaner.do_gc_cycle().then([this, start=std::move(start)] {
	    auto d = std::chrono::steady_clock::now() - start;
	    cleaner.stats.accumulated_gc_duration += d.count();
	  });
	}
      });
    });
}

SegmentCleaner::gc_cycle_ret SegmentCleaner::do_gc_cycle()
{
  if (gc_should_trim_journal()) {
    return gc_trim_journal(
    ).handle_error(
      crimson::ct_error::assert_all{
	"GCProcess::run encountered invalid error in gc_trim_journal"
      }
    );
  } else if (gc_should_trim_backref()) {
    auto start = std::chrono::steady_clock::now();
    return gc_trim_backref(get_backref_tail()
    ).safe_then([this, start=std::move(start)](auto) {
      auto interval = std::chrono::steady_clock::now() - start;
      stats.accumulated_gc_backref_trimming_duration += interval.count();
      stats.gc_backref_trimming_cycles ++;
      return seastar::now();
    }).handle_error(
      crimson::ct_error::assert_all{
	"GCProcess::run encountered invalid error in gc_trim_backref"
      }
    );
  } else if (gc_should_reclaim_space()) {
    return gc_reclaim_space(
    ).handle_error(
      crimson::ct_error::assert_all{
	"GCProcess::run encountered invalid error in gc_reclaim_space"
      }
    );
  } else {
    return seastar::now();
  }
}

SegmentCleaner::gc_trim_backref_ret
SegmentCleaner::gc_trim_backref(journal_seq_t limit) {
  return seastar::do_with(
    journal_seq_t(),
    [this, limit=std::move(limit)](auto &seq) mutable {
    return repeat_eagain([this, limit=std::move(limit), &seq] {
      return ecb->with_transaction_intr(
	Transaction::src_t::TRIM_BACKREF,
	"trim_backref",
	[this, limit](auto &t) {
	return trim_backrefs(
	  t,
	  limit
	).si_then([this, &t, limit](auto trim_backrefs_to)
	  -> ExtentCallbackInterface::submit_transaction_direct_iertr::future<
	    journal_seq_t> {
	  if (trim_backrefs_to != JOURNAL_SEQ_NULL) {
	    return ecb->submit_transaction_direct(
	      t, std::make_optional<journal_seq_t>(trim_backrefs_to)
	    ).si_then([trim_backrefs_to=std::move(trim_backrefs_to)]() mutable {
	      return seastar::make_ready_future<
		journal_seq_t>(std::move(trim_backrefs_to));
	    });
	  }
	  return seastar::make_ready_future<journal_seq_t>(std::move(limit));
	});
      }).safe_then([&seq](auto trim_backrefs_to) {
	seq = std::move(trim_backrefs_to);
      });
    }).safe_then([&seq] {
      return gc_trim_backref_ertr::make_ready_future<
	journal_seq_t>(std::move(seq));
    });
  });
}

SegmentCleaner::gc_trim_journal_ret SegmentCleaner::gc_trim_journal()
{
  return seastar::do_with(
    (size_t)0,
    std::chrono::steady_clock::now(),
    journal_trim_accumulated_stats_t(),
    should_block_on_gc(),
    [this](auto &repeats, auto &start, auto &accum_stats, auto &blocked) {
    return gc_trim_backref(get_dirty_tail()
    ).safe_then([this, &repeats, &start, &accum_stats, &blocked](auto seq) {
      auto backrefs_trimmed = std::chrono::steady_clock::now();
      auto d = backrefs_trimmed - start;
      accum_stats.accumulated_trim_backrefs_time += d.count();
      return repeat_eagain([this, seq=std::move(seq), &repeats, &accum_stats, &blocked,
			    backrefs_trimmed=std::move(backrefs_trimmed)]() mutable {
	auto repeated_start = std::chrono::steady_clock::now();
	repeats++;
	return ecb->with_transaction_intr(
	  Transaction::src_t::CLEANER_TRIM,
	  "trim_journal",
	  [this, seq=std::move(seq), &accum_stats, &blocked,
	  repeated_start=std::move(repeated_start)](auto& t) {
	  return rewrite_dirty(t, seq
	  ).si_then([this, &t, repeated_start, &accum_stats, &blocked] {
	    auto prepare_rewrite_dirty_time = std::chrono::steady_clock::now();
	    auto d = prepare_rewrite_dirty_time - repeated_start;
	    accum_stats.accumulated_prepare_rewrite_dirty_time += d.count();
	    if (blocked)
	      accum_stats.accumulated_io_blocked_prepare_rewrite_dirty_time += d.count();
	    return ecb->submit_transaction_direct(t
	    ).si_then([&accum_stats, &blocked, &t,
		      prepare_rewrite_dirty_time=std::move(prepare_rewrite_dirty_time)] {
	      auto d = std::chrono::steady_clock::now() - prepare_rewrite_dirty_time;
	      accum_stats.accumulated_rewrite_dirty_trans_commit_time += d.count();
	      if (blocked) {
		accum_stats.accumulated_io_blocked_rewrite_dirty_trans_commit_time +=
		  d.count();
		accum_stats.accumulated_io_blocked_committed_extents +=
		  t.inline_block_list.size() + t.ool_block_list.size();
	      }
	    });
	  }).si_then([&accum_stats, repeated_start=std::move(repeated_start)] {
	    auto d = std::chrono::steady_clock::now() - repeated_start;
	    accum_stats.accumulated_rewrite_dirty_time += d.count();
	  });
	});
      });
    }).safe_then([&start, this, &repeats, &accum_stats, &blocked] {
      stats.jt_stats.repeats += repeats;
      stats.jt_stats.cycles++;
      if (blocked)
	stats.jt_stats.cycles_io_blocked++;
      auto d = std::chrono::steady_clock::now() - start;
      stats.jt_stats.duration += d.count();
      stats.jt_stats.accum_stats.add(accum_stats);
    });
  });
}

SegmentCleaner::retrieve_live_extents_ret
SegmentCleaner::_retrieve_live_extents(
  Transaction &t,
  std::set<
    backref_buf_entry_t,
    backref_buf_entry_t::cmp_t> &&backrefs,
  std::vector<CachedExtentRef> &extents)
{
  return seastar::do_with(
    JOURNAL_SEQ_NULL,
    std::move(backrefs),
    [this, &t, &extents](auto &seq, auto &backrefs) {
    return trans_intr::parallel_for_each(
      backrefs,
      [this, &extents, &t, &seq](auto &ent) {
      LOG_PREFIX(SegmentCleaner::_retrieve_live_extents);
      DEBUGT("getting extent of type {} at {}~{}",
	t,
	ent.type,
	ent.paddr,
	ent.len);
      return ecb->get_extent_if_live(
	t, ent.type, ent.paddr, ent.laddr, ent.len
      ).si_then([this, FNAME, &extents, &ent, &seq, &t](auto ext) {
	if (!ext) {
	  DEBUGT("addr {} dead, skipping", t, ent.paddr);
	  auto backref = backref_manager.get_cached_backref_removal(ent.paddr);
	  if (seq == JOURNAL_SEQ_NULL || seq < backref.seq) {
	    seq = backref.seq;
	  }
	} else {
	  extents.emplace_back(std::move(ext));
	}
	return ExtentCallbackInterface::rewrite_extent_iertr::now();
      });
    }).si_then([&seq] {
      return retrieve_live_extents_iertr::make_ready_future<
	journal_seq_t>(std::move(seq));
    });
  });
}

SegmentCleaner::retrieve_backref_mappings_ret
SegmentCleaner::retrieve_backref_mappings(
  paddr_t start_paddr,
  paddr_t end_paddr)
{
  return seastar::do_with(
    backref_pin_list_t(),
    [this, start_paddr, end_paddr](auto &pin_list) {
    return repeat_eagain([this, start_paddr, end_paddr, &pin_list] {
      return ecb->with_transaction_intr(
	Transaction::src_t::READ,
	"get_backref_mappings",
	[this, start_paddr, end_paddr](auto &t) {
	return backref_manager.get_mappings(
	  t, start_paddr, end_paddr
	);
      }).safe_then([&pin_list](auto&& list) {
	pin_list = std::move(list);
      });
    }).safe_then([&pin_list] {
      return seastar::make_ready_future<backref_pin_list_t>(std::move(pin_list));
    });
  });
}

SegmentCleaner::gc_reclaim_space_ret SegmentCleaner::gc_reclaim_space()
{
  LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
  if (!reclaim_state) {
    segment_id_t seg_id = get_next_reclaim_segment();
    auto &segment_info = segments[seg_id];
    INFO("reclaim {} {} start", seg_id, segment_info);
    ceph_assert(segment_info.is_closed());
    reclaim_state = reclaim_state_t::create(
        seg_id, segments.get_segment_size());
  }
  reclaim_state->advance(config.reclaim_bytes_per_cycle);

  DEBUG("reclaiming {}~{}",
        reclaim_state->start_pos,
        reclaim_state->end_pos);
  double pavail_ratio = get_projected_available_ratio();

  return seastar::do_with(
    (size_t)0,
    (size_t)0,
    space_reclaim_accumulated_stats_t(),
    std::chrono::steady_clock::now(),
    std::chrono::steady_clock::time_point(),
    [this, pavail_ratio](
      auto &reclaimed,
      auto &runs,
      auto &accum_stats,
      auto &start,
      auto &repeated_start) {
    return retrieve_backref_mappings(
      reclaim_state->start_pos,
      reclaim_state->end_pos
    ).safe_then([this, &reclaimed, &runs, &start,
		&accum_stats, &repeated_start](auto pin_list) {
      auto got_mappings = std::chrono::steady_clock::now();
      auto d = got_mappings - start;
      accum_stats.accumulated_get_backref_mappings_time += d.count();
      return seastar::do_with(
	std::move(pin_list),
	[this, &reclaimed, &runs, &accum_stats, &start,
	&repeated_start](auto &pin_list) {
	return repeat_eagain(
	  [this, &reclaimed, &runs, &repeated_start,
	  &start, &accum_stats, &pin_list]() mutable {
	  reclaimed = 0;
	  runs++;
	  auto new_repeated_start = std::chrono::steady_clock::now();
	  if (repeated_start.time_since_epoch().count() != 0) {
	    auto d = new_repeated_start - repeated_start;
	    accum_stats.accumulated_time_wasted_on_repeat += d.count();
	  }
	  repeated_start = std::move(new_repeated_start);
	  return seastar::do_with(
	    backref_manager.get_cached_backref_extents_in_range(
	      reclaim_state->start_pos, reclaim_state->end_pos),
	    backref_manager.get_cached_backrefs_in_range(
	      reclaim_state->start_pos, reclaim_state->end_pos),
	    backref_manager.get_cached_backref_removals_in_range(
	      reclaim_state->start_pos, reclaim_state->end_pos),
	    JOURNAL_SEQ_NULL,
	    [this, &reclaimed, &pin_list, &accum_stats, &repeated_start](
	      auto &backref_extents,
	      auto &backrefs,
	      auto &del_backrefs,
	      auto &seq) {
	    return ecb->with_transaction_intr(
	      Transaction::src_t::CLEANER_RECLAIM,
	      "reclaim_space",
	      [this, &backref_extents, &backrefs, &seq,
	      &del_backrefs, &reclaimed, &pin_list, &accum_stats, &repeated_start](auto &t) {
	      LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
	      DEBUGT("{} backrefs, {} del_backrefs, {} pins", t,
		backrefs.size(), del_backrefs.size(), pin_list.size());
	      for (auto &br : backrefs) {
		ceph_assert(br.seq != JOURNAL_SEQ_NULL);
		if (seq == JOURNAL_SEQ_NULL || br.seq > seq)
		  seq = br.seq;
	      }
	      for (auto &pin : pin_list) {
		backrefs.emplace(
		  pin->get_key(),
		  pin->get_val(),
		  pin->get_length(),
		  pin->get_type(),
		  JOURNAL_SEQ_NULL);
	      }
	      accum_stats.accumulated_backrefs_to_be_rewritten += backrefs.size();
	      for (auto &del_backref : del_backrefs) {
		DEBUGT("del_backref {}~{} {} {}", t,
		  del_backref.paddr, del_backref.len, del_backref.type, del_backref.seq);
		auto it = backrefs.find(del_backref.paddr);
		if (it != backrefs.end())
		  backrefs.erase(it);
		ceph_assert(del_backref.seq != JOURNAL_SEQ_NULL);
		if (seq == JOURNAL_SEQ_NULL || del_backref.seq > seq)
		  seq = del_backref.seq;
	      }
	      auto backrefs_calculated = std::chrono::steady_clock::now();
	      auto d = backrefs_calculated - repeated_start;
	      accum_stats.accumulated_backrefs_calc_time += d.count();
	      return seastar::do_with(
		std::vector<CachedExtentRef>(),
		[this, &backref_extents, &backrefs, &reclaimed, &t, &seq,
		&accum_stats, backrefs_calculated=std::move(backrefs_calculated)]
		(auto &extents) {
		return backref_manager.retrieve_backref_extents(
		  t, std::move(backref_extents), extents
		).si_then([this, &extents, &t, &backrefs, &accum_stats,
			  backrefs_calculated=std::move(backrefs_calculated), &seq] {
		  auto got_backref_extents = std::chrono::steady_clock::now();
		  auto d = got_backref_extents - backrefs_calculated;
		  accum_stats.accumulated_get_backref_extents_time += d.count();
		  return _retrieve_live_extents(
		    t, std::move(backrefs), extents
		  ).si_then([this, &seq, &t, &accum_stats,
			    got_backref_extents=std::move(got_backref_extents)](auto nseq) {
		    auto got_live_extents = std::chrono::steady_clock::now();
		    auto d = got_live_extents - got_backref_extents;
		    accum_stats.accumulated_get_live_extents_time += d.count();
		    if (nseq != JOURNAL_SEQ_NULL &&
			(nseq > seq || seq == JOURNAL_SEQ_NULL))
		      seq = nseq;
		    auto fut = BackrefManager::merge_cached_backrefs_iertr::make_ready_future<
		      std::chrono::steady_clock::time_point>(got_live_extents);
		    if (seq != JOURNAL_SEQ_NULL) {
		      fut = backref_manager.merge_cached_backrefs(
			t, seq, std::numeric_limits<uint64_t>::max()
		      ).si_then([&accum_stats, got_live_extents=std::move(got_live_extents)](auto) {
			auto backref_inserted = std::chrono::steady_clock::now();
			auto d = backref_inserted - got_live_extents;
			accum_stats.accumulated_backref_batch_insert_time += d.count();
			return BackrefManager::merge_cached_backrefs_iertr::make_ready_future<
			  std::chrono::steady_clock::time_point>(
			    std::move(backref_inserted));
		      });
		    }
		    return fut;
		  });
		}).si_then([&extents, this, &t, &reclaimed, &accum_stats](
			    auto backref_inserted) {
		  return trans_intr::do_for_each(
		    extents,
		    [this, &t, &reclaimed](auto &ext) {
		    reclaimed += ext->get_length();
		    return ecb->rewrite_extent(t, ext);
		  }).si_then([&accum_stats,
			      backref_inserted=std::move(backref_inserted)] {
		    auto extents_rewritten = std::chrono::steady_clock::now();
		    auto d = extents_rewritten - backref_inserted;
		    accum_stats.accumulated_rewrite_extents_time += d.count();
		    return ExtentCallbackInterface::rewrite_extent_iertr::make_ready_future<
		      std::chrono::steady_clock::time_point>(
			std::move(extents_rewritten));
		  });
		}).si_then([this, &t, &seq, &accum_stats](auto extents_rewritten) {
		  if (reclaim_state->is_complete())
		    t.mark_segment_to_release(reclaim_state->get_segment_id());
		  if (t.get_num_fresh_backref() * BACKREF_NODE_SIZE > 16777216) {
		    LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
		    ERRORT("backref bytes exceeded 16MB: {}",
		      t, t.get_num_fresh_backref() * BACKREF_NODE_SIZE);
		  }
		  return ecb->submit_transaction_direct(
		    t, std::make_optional<journal_seq_t>(std::move(seq))
		  ).si_then([&accum_stats, extents_rewritten=std::move(extents_rewritten)] {
		    auto d = std::chrono::steady_clock::now() - extents_rewritten;
		    accum_stats.accumulated_transaction_submit_time += d.count();
		  });
		});
	      });
	    });
	  });
	});
      });
    }).safe_then(
      [&reclaimed, this, pavail_ratio, start, &runs, &accum_stats] {
      LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
      stats.sr_stats.reclaim_space_cycles++;
#ifndef NDEBUG
      auto ndel_backrefs =
	backref_manager.get_cached_backref_removals_in_range(
	  reclaim_state->start_pos, reclaim_state->end_pos);
      if (!ndel_backrefs.empty()) {
	for (auto &del_br : ndel_backrefs) {
	  ERROR("unexpected del_backref {}~{} {} {}",
	    del_br.paddr, del_br.len, del_br.type, del_br.seq);
	}
	ceph_abort("impossible");
      }
#endif
      stats.sr_stats.reclaiming_bytes += reclaimed;
      auto d = std::chrono::steady_clock::now() - start;
      accum_stats.accumulated_reclaim_space_duration += d.count();
      accum_stats.accumulated_reclaim_space_repeats += runs;
      stats.sr_stats.accum_stats.add(accum_stats);
      if (reclaim_state->is_complete()) {
	INFO("reclaim {} finish, alive/total={}",
             reclaim_state->get_segment_id(),
             stats.reclaiming_bytes/(double)segments.get_segment_size());
	stats.reclaimed_segment_bytes += segments.get_segment_size();
	stats.sr_stats.reclaim_rewrite_bytes += stats.sr_stats.reclaiming_bytes;
	stats.sr_stats.reclaiming_bytes = 0;
	reclaim_state.reset();
      }
    });
  });
}

SegmentCleaner::mount_ret SegmentCleaner::mount()
{
  LOG_PREFIX(SegmentCleaner::mount);
  const auto& sms = sm_group->get_segment_managers();
  INFO("{} segment managers", sms.size());
  init_complete = false;
  stats = {};
  journal_tail_target = JOURNAL_SEQ_NULL;
  journal_tail_committed = JOURNAL_SEQ_NULL;
  dirty_extents_replay_from = JOURNAL_SEQ_NULL;
  alloc_info_replay_from = JOURNAL_SEQ_NULL;
  
  space_tracker.reset(
    detailed ?
    (SpaceTrackerI*)new SpaceTrackerDetailed(
      sms) :
    (SpaceTrackerI*)new SpaceTrackerSimple(
      sms));
  
  segments.reset();
  for (auto sm : sms) {
    segments.add_segment_manager(*sm);
  }
  metrics.clear();
  register_metrics();

  INFO("{} segments", segments.get_num_segments());
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this, FNAME](auto& segment_set) {
    return crimson::do_for_each(
      segments.begin(),
      segments.end(),
      [this, FNAME, &segment_set](auto& it) {
	auto segment_id = it.first;
	return sm_group->read_segment_header(
	  segment_id
	).safe_then([segment_id, this, FNAME, &segment_set](auto header) {
	  INFO("segment_id={} -- {}", segment_id, header);
	  auto s_type = header.get_type();
	  if (s_type == segment_type_t::NULL_SEG) {
	    ERROR("got null segment, segment_id={} -- {}", segment_id, header);
	    ceph_abort();
	  }
	  return sm_group->read_segment_tail(
	    segment_id
	  ).safe_then([this, segment_id, &segment_set, header](auto tail)
	    -> scan_extents_ertr::future<> {
	    if (tail.segment_nonce != header.segment_nonce) {
	      return scan_nonfull_segment(header, segment_set, segment_id);
	    }
	    time_point last_modified(duration(tail.last_modified));
	    time_point last_rewritten(duration(tail.last_rewritten));
	    segments.update_last_modified_rewritten(
                segment_id, last_modified, last_rewritten);
	    if (tail.get_type() == segment_type_t::JOURNAL) {
	      update_journal_tail_committed(tail.journal_tail);
	      update_journal_tail_target(
		tail.journal_tail,
		tail.alloc_replay_from);
	    }
	    init_mark_segment_closed(
	      segment_id,
	      header.segment_seq,
	      header.type);
	    return seastar::now();
	  }).handle_error(
	    crimson::ct_error::enodata::handle(
	      [this, header, segment_id, &segment_set](auto) {
	      return scan_nonfull_segment(header, segment_set, segment_id);
	    }),
	    crimson::ct_error::pass_further_all{}
	  );
	}).handle_error(
	  crimson::ct_error::enoent::handle([](auto) {
	    return mount_ertr::now();
	  }),
	  crimson::ct_error::enodata::handle([](auto) {
	    return mount_ertr::now();
	  }),
	  crimson::ct_error::input_output_error::pass_further{},
	  crimson::ct_error::assert_all{"unexpected error"}
	);
      });
  });
}

SegmentCleaner::scan_extents_ret SegmentCleaner::scan_nonfull_segment(
  const segment_header_t& header,
  scan_extents_ret_bare& segment_set,
  segment_id_t segment_id)
{
  return seastar::do_with(
    scan_valid_records_cursor({
      segments[segment_id].seq,
      paddr_t::make_seg_paddr(segment_id, 0)}),
    [this, segment_id, segment_header=header](auto& cursor) {
    return seastar::do_with(
	SegmentManagerGroup::found_record_handler_t(
	[this, segment_id, segment_header](
	  record_locator_t locator,
	  const record_group_header_t& header,
	  const bufferlist& mdbuf
	) mutable -> SegmentManagerGroup::scan_valid_records_ertr::future<> {
	LOG_PREFIX(SegmentCleaner::scan_nonfull_segment);
	if (segment_header.get_type() == segment_type_t::OOL) {
	  DEBUG("out-of-line segment {}, decodeing {} records",
	    segment_id,
	    header.records);
	  auto maybe_headers = try_decode_record_headers(header, mdbuf);
	  if (!maybe_headers) {
	    ERROR("unable to decode record headers for record group {}",
	      locator.record_block_base);
	    return crimson::ct_error::input_output_error::make();
	  }

	  for (auto& header : *maybe_headers) {
	    mod_time_point_t ctime = header.commit_time;
	    auto commit_type = header.commit_type;
	    if (!ctime) {
	      ERROR("SegmentCleaner::scan_nonfull_segment: extent {} 0 commit_time",
		ctime);
	      ceph_abort("0 commit_time");
	    }
	    time_point commit_time{duration(ctime)};
	    assert(commit_type == record_commit_type_t::MODIFY
	      || commit_type == record_commit_type_t::REWRITE);
	    if (commit_type == record_commit_type_t::MODIFY) {
              segments.update_last_modified_rewritten(segment_id, commit_time, {});
	    }
	    if (commit_type == record_commit_type_t::REWRITE) {
              segments.update_last_modified_rewritten(segment_id, {}, commit_time);
	    }
	  }
	} else {
	  DEBUG("inline segment {}, decodeing {} records",
	    segment_id,
	    header.records);
	  auto maybe_record_deltas_list = try_decode_deltas(
	    header, mdbuf, locator.record_block_base);
	  if (!maybe_record_deltas_list) {
	    ERROR("unable to decode deltas for record {} at {}",
		  header, locator);
	    return crimson::ct_error::input_output_error::make();
	  }
	  for (auto &record_deltas : *maybe_record_deltas_list) {
	    for (auto &[ctime, delta] : record_deltas.deltas) {
	      if (delta.type == extent_types_t::ALLOC_TAIL) {
		journal_seq_t seq;
		decode(seq, delta.bl);
		update_alloc_info_replay_from(seq);
	      }
	    }
	  }
	}
	return seastar::now();
      }),
      [&cursor, segment_header, this](auto& handler) {
	return sm_group->scan_valid_records(
	  cursor,
	  segment_header.segment_nonce,
	  segments.get_segment_size(),
	  handler);
      }
    );
  }).safe_then([this, segment_id, header](auto) {
    init_mark_segment_closed(
      segment_id,
      header.segment_seq,
      header.type);
    return seastar::now();
  });
}

SegmentCleaner::release_ertr::future<>
SegmentCleaner::maybe_release_segment(Transaction &t)
{
  auto to_release = t.get_segment_to_release();
  if (to_release != NULL_SEG_ID) {
    LOG_PREFIX(SegmentCleaner::maybe_release_segment);
    INFOT("releasing segment {}", t, to_release);
    return sm_group->release_segment(to_release
    ).safe_then([this, FNAME, &t, to_release] {
      auto old_usage = calc_utilization(to_release);
      ceph_assert(old_usage == 0);
      segments.mark_empty(to_release);
      auto new_usage = calc_utilization(to_release);
      adjust_segment_util(old_usage, new_usage);
      INFOT("released, should_block_on_gc {}, projected_avail_ratio {}, "
           "reclaim_ratio {}",
           t,
           should_block_on_gc(),
           get_projected_available_ratio(),
           get_reclaim_ratio());
      if (space_tracker->get_usage(to_release) != 0) {
        space_tracker->dump_usage(to_release);
        ceph_abort();
      }
      maybe_wake_gc_blocked_io();
    });
  } else {
    return SegmentManager::release_ertr::now();
  }
}

void SegmentCleaner::complete_init()
{
  LOG_PREFIX(SegmentCleaner::complete_init);
  if (disable_trim) {
    init_complete = true;
    return;
  }
  INFO("done, start GC");
  ceph_assert(segments.get_journal_head() != JOURNAL_SEQ_NULL);
  init_complete = true;
  gc_process.start();
}

void SegmentCleaner::mark_space_used(
  paddr_t addr,
  extent_len_t len,
  time_point last_modified,
  time_point last_rewritten,
  bool init_scan)
{
  LOG_PREFIX(SegmentCleaner::mark_space_used);
  if (addr.get_addr_type() != addr_types_t::SEGMENT) {
    return;
  }
  auto& seg_addr = addr.as_seg_paddr();

  if (!init_scan && !init_complete) {
    return;
  }

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
  DEBUG("segment {} new len: {}~{}, live_bytes: {}",
        seg_addr.get_segment_id(),
        addr,
        len,
        space_tracker->get_usage(seg_addr.get_segment_id()));
}

void SegmentCleaner::mark_space_free(
  paddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(SegmentCleaner::mark_space_free);
  if (!init_complete) {
    return;
  }
  if (addr.get_addr_type() != addr_types_t::SEGMENT) {
    return;
  }

  ceph_assert(stats.used_bytes >= len);
  stats.used_bytes -= len;
  auto& seg_addr = addr.as_seg_paddr();

  DEBUG("segment {} free len: {}~{}",
        seg_addr.get_segment_id(), addr, len);
  auto old_usage = calc_utilization(seg_addr.get_segment_id());
  [[maybe_unused]] auto ret = space_tracker->release(
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    len);
  auto new_usage = calc_utilization(seg_addr.get_segment_id());
  adjust_segment_util(old_usage, new_usage);
  maybe_wake_gc_blocked_io();
  assert(ret >= 0);
  DEBUG("segment {} free len: {}~{}, live_bytes: {}",
        seg_addr.get_segment_id(),
        addr,
        len,
        space_tracker->get_usage(seg_addr.get_segment_id()));
}

segment_id_t SegmentCleaner::get_next_reclaim_segment() const
{
  LOG_PREFIX(SegmentCleaner::get_next_reclaim_segment);
  segment_id_t id = NULL_SEG_ID;
  double max_benefit_cost = 0;
  for (auto& [_id, segment_info] : segments) {
    if (segment_info.is_closed() &&
        !segment_info.is_in_journal(journal_tail_committed)) {
      double benefit_cost = calc_gc_benefit_cost(_id);
      if (benefit_cost > max_benefit_cost) {
        id = _id;
        max_benefit_cost = benefit_cost;
      }
    }
  }
  if (id != NULL_SEG_ID) {
    DEBUG("segment {}, benefit_cost {}",
          id, max_benefit_cost);
    return id;
  } else {
    ceph_assert(get_segments_reclaimable() == 0);
    // see gc_should_reclaim_space()
    ceph_abort("impossible!");
    return NULL_SEG_ID;
  }
}

void SegmentCleaner::log_gc_state(const char *caller) const
{
  LOG_PREFIX(SegmentCleaner::log_gc_state);
  if (LOCAL_LOGGER.is_enabled(seastar::log_level::debug) &&
      !disable_trim) {
    DEBUG(
      "caller {}, "
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

seastar::future<>
SegmentCleaner::reserve_projected_usage(std::size_t projected_usage)
{
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
  return seastar::do_with(
    std::chrono::steady_clock::time_point(),
    [this, projected_usage, is_blocked](auto &maybe_block_begin) {
    return seastar::do_until(
      [this, &maybe_block_begin] {
	log_gc_state("await_hard_limits");
	bool block = should_block_on_gc();
	if (block && !maybe_block_begin.time_since_epoch().count()) {
	  maybe_block_begin = std::chrono::steady_clock::now();
	}
	return !block;
      },
      [this] {
	blocked_io_wake = seastar::promise<>();
	return blocked_io_wake->get_future();
      }
    ).then([this, projected_usage, is_blocked, &maybe_block_begin] {
      ceph_assert(!blocked_io_wake);
      stats.projected_used_bytes += projected_usage;
      ++stats.projected_count;
      stats.projected_used_bytes_sum += stats.projected_used_bytes;
      if (is_blocked) {
	ceph_assert(maybe_block_begin.time_since_epoch().count());
	auto d = std::chrono::steady_clock::now() - maybe_block_begin;
	stats.accumulated_io_block_time += d.count();
	assert(stats.io_blocking_num > 0);
	--stats.io_blocking_num;
      }
    });
  });
}

void SegmentCleaner::release_projected_usage(std::size_t projected_usage)
{
  if (disable_trim) return;
  ceph_assert(init_complete);
  ceph_assert(stats.projected_used_bytes >= projected_usage);
  stats.projected_used_bytes -= projected_usage;
  return maybe_wake_gc_blocked_io();
}

}
