// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/metrics.hh>

#include "crimson/common/log.h"
#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_cleaner);
  }
}

SET_SUBSYS(seastore_cleaner);

namespace crimson::os::seastore {

void segment_info_set_t::segment_info_t::set_open(segment_seq_t seq) {
  assert(state == Segment::segment_state_t::EMPTY);
  assert(segment_seq_to_type(seq) != segment_type_t::NULL_SEG);
  state = Segment::segment_state_t::OPEN;
  journal_segment_seq = seq;
}

void segment_info_set_t::segment_info_t::set_empty() {
  assert(state == Segment::segment_state_t::CLOSED);
  state = Segment::segment_state_t::EMPTY;
  journal_segment_seq = NULL_SEG_SEQ;
}

void segment_info_set_t::segment_info_t::set_closed() {
  state = Segment::segment_state_t::CLOSED;
}

bool SpaceTrackerSimple::equals(const SpaceTrackerI &_other) const
{
  const auto &other = static_cast<const SpaceTrackerSimple&>(_other);

  if (other.live_bytes_by_segment.size() != live_bytes_by_segment.size()) {
    logger().error("{}: different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = live_bytes_by_segment.begin(), j = other.live_bytes_by_segment.begin();
       i != live_bytes_by_segment.end(); ++i, ++j) {
    if (i->second.live_bytes != j->second.live_bytes) {
      all_match = false;
      logger().debug(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->first,
	i->second.live_bytes,
	j->second.live_bytes);
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
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (bitmap[i]) {
      if (!error) {
	logger().error(
	  "SegmentMap::allocate found allocated in {}, {} ~ {}",
	  segment,
	  offset,
	  len);
	error = true;
      }
      logger().debug(
	"SegmentMap::allocate block {} allocated",
	i * block_size);
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
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (!bitmap[i]) {
      if (!error) {
	logger().error(
	  "SegmentMap::release found unallocated in {}, {} ~ {}",
	  segment,
	  offset,
	  len);
	error = true;
      }
      logger().debug(
	"SegmentMap::release block {} unallocated",
	i * block_size);
    }
    bitmap[i] = false;
  }
  return update_usage(-(int64_t)len);
}

bool SpaceTrackerDetailed::equals(const SpaceTrackerI &_other) const
{
  const auto &other = static_cast<const SpaceTrackerDetailed&>(_other);

  if (other.segment_usage.size() != segment_usage.size()) {
    logger().error("{}: different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = segment_usage.begin(), j = other.segment_usage.begin();
       i != segment_usage.end(); ++i, ++j) {
    if (i->second.get_usage() != j->second.get_usage()) {
      all_match = false;
      logger().error(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->first,
	i->second.get_usage(),
	j->second.get_usage());
    }
  }
  return all_match;
}

void SpaceTrackerDetailed::SegmentMap::dump_usage(extent_len_t block_size) const
{
  for (unsigned i = 0; i < bitmap.size(); ++i) {
    if (bitmap[i]) {
      logger().debug("    {} still live", i * block_size);
    }
  }
}

void SpaceTrackerDetailed::dump_usage(segment_id_t id) const
{
  logger().debug("SpaceTrackerDetailed::dump_usage {}", id);
  segment_usage[id].dump_usage(
    block_size_by_segment_manager[id.device_id()]);
}

void SpaceTrackerSimple::dump_usage(segment_id_t id) const
{
  logger().info(
    "SpaceTrackerSimple::dump_usage id: {}, live_bytes: {}",
    id,
    live_bytes_by_segment[id].live_bytes);
}

SegmentCleaner::SegmentCleaner(
  config_t config,
  ExtentReaderRef&& scr,
  backref::BackrefManager &backref_manager,
  Cache &cache,
  bool detailed)
  : detailed(detailed),
    config(config),
    scanner(std::move(scr)),
    backref_manager(backref_manager),
    cache(cache),
    gc_process(*this)
{}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  stats.segment_util.buckets.resize(11);
  for (int i = 0; i < 11; i++) {
    stats.segment_util.buckets[i].upper_bound = ((double)(i + 1)) / 10;
    if (!i) {
      stats.segment_util.buckets[i].count = segments.num_segments();
    } else {
      stats.segment_util.buckets[i].count = 0;
    }
  }
  metrics.add_group("segment_cleaner", {
    sm::make_counter("segments_released", stats.segments_released,
		     sm::description("total number of extents released by SegmentCleaner")),
    sm::make_counter("accumulated_blocked_ios", stats.accumulated_blocked_ios,
		     sm::description("accumulated total number of ios that were blocked by gc")),
    sm::make_counter("reclaimed_segments", stats.reclaimed_segments,
		     sm::description("reclaimed segments")),
    sm::make_counter("reclaim_rewrite_bytes", stats.reclaim_rewrite_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("reclaiming_bytes", stats.reclaiming_bytes,
		     sm::description("bytes being reclaimed")),
    sm::make_derive("empty_segments", stats.empty_segments,
		    sm::description("current empty segments")),
    sm::make_derive("ios_blocking", stats.ios_blocking,
		    sm::description("IOs that are blocking on space usage")),
    sm::make_derive("used_bytes", stats.used_bytes,
		    sm::description("the size of the space occupied by live extents")),
    sm::make_derive("projected_used_bytes", stats.projected_used_bytes,
		    sm::description("the size of the space going to be occupied by new extents")),
    sm::make_derive("avail_bytes",
		    [this] {
		      return segments.get_available_bytes();
		    },
		    sm::description("the size of the space not occupied")),
    sm::make_derive("opened_segments",
		    [this] {
		      return segments.get_opened_segments();
		    },
		    sm::description("the number of segments whose state is open")),
    sm::make_histogram("segment_utilization_distribution",
		       [this]() -> seastar::metrics::histogram& {
		         return stats.segment_util;
		       },
		       sm::description("utilization distribution of all segments"))
  });
}

segment_id_t SegmentCleaner::get_segment(
    device_id_t device_id, segment_seq_t seq)
{
  LOG_PREFIX(SegmentCleaner::get_segment);
  assert(segment_seq_to_type(seq) != segment_type_t::NULL_SEG);
  for (auto it = segments.device_begin(device_id);
       it != segments.device_end(device_id);
       ++it) {
    auto seg_id = it->first;
    auto& segment_info = it->second;
    if (segment_info.is_empty()) {
      DEBUG("returning segment {} {}", seg_id, segment_seq_printer_t{seq});
      mark_open(seg_id, seq);
      return seg_id;
    }
  }
  ERROR("(TODO) handle out of space from device {} with segment_seq={}",
        device_id, segment_seq_printer_t{seq});
  ceph_abort();
  return NULL_SEG_ID;
}

void SegmentCleaner::update_journal_tail_target(journal_seq_t target)
{
  logger().debug(
    "{}: {}, current tail target {}",
    __func__,
    target,
    journal_tail_target);
  assert(journal_tail_target == JOURNAL_SEQ_NULL || target >= journal_tail_target);
  if (journal_tail_target == JOURNAL_SEQ_NULL || target > journal_tail_target) {
    journal_tail_target = target;
  }
  gc_process.maybe_wake_on_space_used();
  maybe_wake_gc_blocked_io();
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  if (journal_tail_committed == JOURNAL_SEQ_NULL ||
      committed > journal_tail_committed) {
    logger().debug(
      "{}: update journal_tail_committed {}",
      __func__,
      committed);
    journal_tail_committed = committed;
  }
  if (journal_tail_target == JOURNAL_SEQ_NULL ||
      committed > journal_tail_target) {
    logger().debug(
      "{}: update journal_tail_target {}",
      __func__,
      committed);
    journal_tail_target = committed;
  }
}

void SegmentCleaner::close_segment(segment_id_t segment)
{
  ceph_assert(segment_seq_to_type(segments[segment].journal_segment_seq) !=
              segment_type_t::NULL_SEG);
  mark_closed(segment);
}

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  return ecb->get_next_dirty_extents(
    t,
    limit,
    config.journal_rewrite_per_cycle
  ).si_then([=, &t](auto dirty_list) {
    LOG_PREFIX(SegmentCleaner::rewrite_dirty);
    DEBUGT("rewrite {} dirty extents", t, dirty_list.size());
    return seastar::do_with(
      std::move(dirty_list),
      [this, &t, limit](auto &dirty_list) {
	journal_seq_t seq_to_rm = dirty_list.empty()
	  ? limit
	  : dirty_list.back()->get_dirty_from();
	return backref_manager.batch_insert_from_cache(
	  t,
	  seq_to_rm
	).si_then([this, &t, &dirty_list] {
	  return trans_intr::do_for_each(
	    dirty_list,
	    [this, &t](auto &e) {
	    LOG_PREFIX(SegmentCleaner::rewrite_dirty);
	    DEBUGT("cleaning {}", t, *e);
	    if (e->get_type() >= extent_types_t::BACKREF_INTERNAL) {
	      return backref_manager.rewrite_extent(t, e);
	    } else {
	      return ecb->rewrite_extent(
		t, e
	      ).si_then([e, this, &t] {
		if (e->is_logical() || is_lba_node(e->get_type())) {
		  t.dont_record_release(e);
		  return backref_manager.remove_mapping(
		    t, e->get_paddr()).si_then([](auto) {
		    return seastar::now();
		  }).handle_error_interruptible(
		    crimson::ct_error::input_output_error::pass_further(),
		    crimson::ct_error::assert_all()
		  );
		} else {
		  return ExtentCallbackInterface::rewrite_extent_iertr::now();
		}
	      });
	    }
	  });
	}).si_then([seq_to_rm] {
	  return rewrite_dirty_iertr::make_ready_future<
	    journal_seq_t>(std::move(seq_to_rm));
	});
      });
  });
}

SegmentCleaner::gc_cycle_ret SegmentCleaner::GCProcess::run()
{
  return seastar::do_until(
    [this] { return stopping; },
    [this] {
      return maybe_wait_should_run(
      ).then([this] {
	cleaner.log_gc_state("GCProcess::run");

	if (stopping) {
	  return seastar::now();
	} else {
	  return cleaner.do_gc_cycle();
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

SegmentCleaner::gc_trim_journal_ret SegmentCleaner::gc_trim_journal()
{
  return repeat_eagain([this] {
    return ecb->with_transaction_intr(
      Transaction::src_t::CLEANER_TRIM,
      "trim_journal",
      [this](auto& t)
    {
      return rewrite_dirty(t, get_dirty_tail()
      ).si_then([this, &t](auto seq_to_rm) {
        return ecb->submit_transaction_direct(
	  t, std::make_optional<journal_seq_t>(std::move(seq_to_rm)));
      });
    });
  });
}

SegmentCleaner::gc_reclaim_space_ret SegmentCleaner::gc_reclaim_space()
{
  journal_seq_t next = get_next_gc_target();
  auto &seg_paddr = next.offset.as_seg_paddr();
  auto &sm_info = segments[seg_paddr.get_segment_id().device_id()];
  auto segment_id = seg_paddr.get_segment_id();
  segment_id_t next_segment_id{
    segment_id.device_id(),
    segment_id.device_segment_id() + 1};
  auto end_paddr = paddr_t::make_seg_paddr(next_segment_id, 0);

  auto backref_extents = cache.get_backref_extents_in_range(
    next.offset, end_paddr);
  auto backrefs = cache.get_backrefs_in_range(next.offset, end_paddr);
  auto del_backrefs = cache.get_del_backrefs_in_range(
    next.offset, end_paddr);

  return seastar::do_with(
    std::move(backref_extents),
    std::move(backrefs),
    std::move(del_backrefs),
    (size_t)0,
    [this, &sm_info, next, segment_id](
      auto &backref_extents,
      auto &backrefs,
      auto &del_backrefs,
      auto &reclaimed) {
    return repeat_eagain(
      [this, &backref_extents, &backrefs, &reclaimed,
      &del_backrefs, next, &sm_info, segment_id]() mutable {
      reclaimed = 0;
      return ecb->with_transaction_intr(
	Transaction::src_t::CLEANER_RECLAIM,
	"reclaim_space",
	[segment_id, this, &backref_extents, &backrefs,
	&del_backrefs, &reclaimed, next, &sm_info](auto &t) {
	return backref_manager.get_mappings(
	  t, next.offset, sm_info->segment_size
	).si_then(
	  [segment_id, this, &backref_extents, &backrefs,
	  &del_backrefs, &reclaimed, &t](auto pin_list) {
	  for (auto &pin : pin_list) {
	    backrefs.emplace(
	      pin->get_key(),
	      pin->get_val(),
	      pin->get_length(),
	      pin->get_type(),
	      journal_seq_t());
	  }
	  journal_seq_t seq = JOURNAL_SEQ_NULL;
	  for (auto &del_backref : del_backrefs) {
	    auto it = backrefs.find(del_backref.paddr);
	    if (it != backrefs.end())
	      backrefs.erase(it);
	    if (seq == JOURNAL_SEQ_NULL
		|| it->seq > seq)
	      seq = it->seq;
	  }
	  return seastar::do_with(
	    std::vector<CachedExtentRef>(),
	    [this, &backref_extents, &backrefs, &reclaimed, &t](auto &extents) {
	    return trans_intr::parallel_for_each(
	      backref_extents,
	      [this, &extents, &t](auto &ent) {
	      // only the gc fiber which is single can rewrite backref extents,
	      // so it must be alive
	      assert(is_backref_node(ent.type));
	      LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
	      DEBUGT("getting backref extent of type {} at {}",
		t,
		ent.type,
		ent.paddr);
	      return cache.get_extent_by_type(
		t, ent.type, ent.paddr, L_ADDR_NULL, BACKREF_NODE_SIZE
	      ).si_then([&extents](auto ext) {
		extents.emplace_back(std::move(ext));
	      });
	    }).si_then([this, &extents, &t, &backrefs] {
	      return trans_intr::do_for_each(
		backrefs,
		[this, &extents, &t](auto &ent) {
		LOG_PREFIX(SegmentCleaner::gc_reclaim_space);
		DEBUGT("getting extent of type {} at {}~{}",
		  t,
		  ent.type,
		  ent.paddr,
		  ent.len);
		return ecb->get_extent_if_live(
		  t, ent.type, ent.paddr, ent.laddr, ent.len
		).si_then([&extents, &ent](auto ext) {
		  if (!ext) {
		    logger().debug(
		      "SegmentCleaner::gc_reclaim_space: addr {} dead, skipping",
		      ent.paddr);
		  } else {
		    extents.emplace_back(std::move(ext));
		  }
		  return ExtentCallbackInterface::rewrite_extent_iertr::now();
		});
	      });
	    }).si_then([&extents, this, &t, &reclaimed] {
	      return trans_intr::do_for_each(
		extents,
		[this, &t, &reclaimed](auto &ext) {
		reclaimed += ext->get_length();
		if (ext->get_type() >= extent_types_t::BACKREF_INTERNAL) {
		  return backref_manager.rewrite_extent(t, ext);
		} else {
		  return ecb->rewrite_extent(
		    t, ext
		  ).si_then([ext, this, &t] {
		    t.dont_record_release(ext);
		    return backref_manager.remove_mapping(
		      t, ext->get_paddr()).si_then([](auto) {
		      return seastar::now();
		    }).handle_error_interruptible(
		      crimson::ct_error::input_output_error::pass_further(),
		      crimson::ct_error::assert_all()
		    );
		  });
		}
	      });
	    });
	  }).si_then([this, seq, &t] {
	    if (seq != JOURNAL_SEQ_NULL) {
	      return backref_manager.batch_insert_from_cache(
		t, seq);
	    }
	    return backref::BackrefManager::batch_insert_iertr::now();
	  }).si_then([this, &t, segment_id] {
	    t.mark_segment_to_release(segment_id);
	    return ecb->submit_transaction_direct(t);
	  });
	});
      });
    }).safe_then([&reclaimed, this] {
      stats.reclaim_rewrite_bytes += reclaimed;
      stats.reclaimed_segments++;
    });
  });

/*  if (!scan_cursor) {
    journal_seq_t next = get_next_gc_target();
    if (next == JOURNAL_SEQ_NULL) {
      logger().debug(
	"SegmentCleaner::do_gc: no segments to gc");
      return seastar::now();
    }
    scan_cursor =
      std::make_unique<ExtentReader::scan_extents_cursor>(
	next);
    logger().debug(
      "SegmentCleaner::do_gc: starting gc on segment {}",
      scan_cursor->seq);
  } else {
    ceph_assert(!scan_cursor->is_complete());
  }

  return scanner->scan_extents(
    *scan_cursor,
    config.reclaim_bytes_stride
  ).safe_then([this](auto &&_extents) {
    return seastar::do_with(
        std::move(_extents),
	(size_t)0,
        [this](auto &extents, auto &reclaimed) {
      return repeat_eagain([this, &extents, &reclaimed]() mutable {
	reclaimed = 0;
        logger().debug(
          "SegmentCleaner::gc_reclaim_space: processing {} extents",
          extents.size());
        return ecb->with_transaction_intr(
          Transaction::src_t::CLEANER_RECLAIM,
          "reclaim_space",
          [this, &extents, &reclaimed](auto& t)
        {
          return trans_intr::do_for_each(
              extents,
              [this, &t, &reclaimed](auto &extent) {
	    auto &addr = extent.first;
	    auto commit_time = extent.second.first.commit_time;
	    auto commit_type = extent.second.first.commit_type;
	    auto &info = extent.second.second;
            logger().debug(
              "SegmentCleaner::gc_reclaim_space: checking extent {}",
              info);
            return ecb->get_extent_if_live(
              t,
              info.type,
              addr,
              info.addr,
              info.len
            ).si_then([&info, commit_type, commit_time, addr=addr, &t, this, &reclaimed]
	      (CachedExtentRef ext) {
              if (!ext) {
                logger().debug(
                  "SegmentCleaner::gc_reclaim_space: addr {} dead, skipping",
                  addr);
                return ExtentCallbackInterface::rewrite_extent_iertr::now();
              } else {
                logger().debug(
                  "SegmentCleaner::gc_reclaim_space: addr {} alive, gc'ing {}",
                  addr,
                  *ext);
		assert(commit_time);
		assert(info.last_modified);
		assert(commit_type == record_commit_type_t::MODIFY
		  || commit_type == record_commit_type_t::REWRITE);
		if (ext->get_last_modified() ==
		    seastar::lowres_system_clock::time_point()) {
		  assert(ext->get_last_rewritten() ==
		    seastar::lowres_system_clock::time_point());
		  ext->set_last_modified(
		    seastar::lowres_system_clock::duration(
		      info.last_modified));
		}
		if (commit_type == record_commit_type_t::REWRITE
		    && ext->get_last_rewritten() ==
		      seastar::lowres_system_clock::time_point()) {
		  ext->set_last_rewritten(
		    seastar::lowres_system_clock::duration(
		      commit_time));
		}

		assert(
		  (commit_type == record_commit_type_t::MODIFY
		    && commit_time <=
		      ext->get_last_modified().time_since_epoch().count())
		  || (commit_type == record_commit_type_t::REWRITE
		      && commit_time ==
			ext->get_last_rewritten().time_since_epoch().count()));

		reclaimed += ext->get_length();
                return ecb->rewrite_extent(
                  t,
                  ext);
              }
            });
          }).si_then([this, &t] {
            if (scan_cursor->is_complete()) {
              t.mark_segment_to_release(scan_cursor->get_segment_id());
            }
            return ecb->submit_transaction_direct(t);
          });
        });
      }).safe_then([&reclaimed] {
	return seastar::make_ready_future<size_t>(reclaimed);
      });
    });
  }).safe_then([this](size_t reclaimed) {
    stats.reclaiming_bytes += reclaimed;
    if (scan_cursor->is_complete()) {
      stats.reclaim_rewrite_bytes += stats.reclaiming_bytes;
      stats.reclaiming_bytes = 0;
      stats.reclaimed_segments++;
      scan_cursor.reset();
    }
  });
*/
}

SegmentCleaner::mount_ret SegmentCleaner::mount(
  device_id_t pdevice_id,
  std::vector<SegmentManager*>& sms)
{
  logger().debug(
    "SegmentCleaner::mount: {} segment managers", sms.size());
  init_complete = false;
  stats = {};
  journal_tail_target = JOURNAL_SEQ_NULL;
  journal_tail_committed = JOURNAL_SEQ_NULL;
  journal_head = JOURNAL_SEQ_NULL;
  journal_device_id = pdevice_id;
  
  space_tracker.reset(
    detailed ?
    (SpaceTrackerI*)new SpaceTrackerDetailed(
      sms) :
    (SpaceTrackerI*)new SpaceTrackerSimple(
      sms));
  
  segments.clear();
  for (auto sm : sms) {
    // sms is a vector that is indexed by device id and
    // always has "max_device" elements, some of which
    // may be null.
    if (!sm) {
      continue;
    }
    segments.add_segment_manager(*sm);
    stats.empty_segments += sm->get_num_segments();
  }
  metrics.clear();
  register_metrics();

  logger().debug("SegmentCleaner::mount: {} segments", segments.size());
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this](auto& segment_set) {
    return crimson::do_for_each(
      segments.begin(),
      segments.end(),
      [this, &segment_set](auto& it) {
	auto segment_id = it.first;
	return scanner->read_segment_header(
	  segment_id
	).safe_then([segment_id, this, &segment_set](auto header) {
	  logger().debug(
	    "ExtentReader::mount: segment_id={} -- {}",
	    segment_id, header);
	  auto s_type = header.get_type();
	  if (s_type == segment_type_t::NULL_SEG) {
	    logger().error(
	      "ExtentReader::mount: got null segment, segment_id={} -- {}",
	      segment_id, header);
	    ceph_abort();
	  }
	  return scanner->read_segment_tail(
	    segment_id
	  ).safe_then([this, segment_id, &segment_set, header](auto tail)
	    -> scan_extents_ertr::future<> {
	    if (tail.segment_nonce != header.segment_nonce) {
	      return scan_nonfull_segment(header, segment_set, segment_id);
	    }
	    seastar::lowres_system_clock::time_point last_modified(
	      seastar::lowres_system_clock::duration(tail.last_modified));
	    seastar::lowres_system_clock::time_point last_rewritten(
	      seastar::lowres_system_clock::duration(tail.last_rewritten));
	    if (segments[segment_id].last_modified < last_modified) {
	      segments[segment_id].last_modified = last_modified;
	    }
	    if (segments[segment_id].last_rewritten < last_rewritten) {
	      segments[segment_id].last_rewritten = last_rewritten;
	    }
	    if (tail.get_type() == segment_type_t::JOURNAL) {
	      update_journal_tail_committed(tail.journal_tail);
	    }
	    init_mark_segment_closed(
	      segment_id,
	      header.journal_segment_seq);
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
  if (header.get_type() == segment_type_t::OOL) {
    logger().info(
      "ExtentReader::init_segments: out-of-line segment {}",
      segment_id);
    return seastar::do_with(
      scan_valid_records_cursor({
	segments[segment_id].journal_segment_seq,
	paddr_t::make_seg_paddr(segment_id, 0)}),
      [this, segment_id, header](auto& cursor) {
      return seastar::do_with(
	ExtentReader::found_record_handler_t([this, segment_id](
	    record_locator_t locator,
	    const record_group_header_t& header,
	    const bufferlist& mdbuf
	  ) mutable -> ExtentReader::scan_valid_records_ertr::future<> {
	  LOG_PREFIX(SegmentCleaner::scan_nonfull_segment);
	  DEBUG("decodeing {} records", header.records);
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
	      ERROR("Scanner::init_segments: extent {} 0 commit_time",
		ctime);
	      ceph_abort("0 commit_time");
	    }
	    seastar::lowres_system_clock::time_point commit_time{
	      seastar::lowres_system_clock::duration(ctime)};
	    assert(commit_type == record_commit_type_t::MODIFY
	      || commit_type == record_commit_type_t::REWRITE);
	    if (commit_type == record_commit_type_t::MODIFY
		&& this->segments[segment_id].last_modified < commit_time) {
	      this->segments[segment_id].last_modified = commit_time;
	    }
	    if (commit_type == record_commit_type_t::REWRITE
		&& this->segments[segment_id].last_rewritten < commit_time) {
	      this->segments[segment_id].last_rewritten = commit_time;
	    }
	  }
	  return seastar::now();
	}),
	[&cursor, header, segment_id, this](auto& handler) {
	  return scanner->scan_valid_records(
	    cursor,
	    header.segment_nonce,
	    segments[segment_id.device_id()]->segment_size,
	    handler);
	}
      );
    }).safe_then([this, segment_id, header](auto) {
      init_mark_segment_closed(
	segment_id,
	header.journal_segment_seq);
      return seastar::now();
    });
  } else if (header.get_type() == segment_type_t::JOURNAL) {
    logger().info(
      "ExtentReader::init_segments: journal segment {}",
      segment_id);
    segment_set.emplace_back(std::make_pair(segment_id, std::move(header)));
  } else {
    ceph_abort("unexpected segment type");
  }
  init_mark_segment_closed(
    segment_id,
    header.journal_segment_seq);
  return seastar::now();
}

}
