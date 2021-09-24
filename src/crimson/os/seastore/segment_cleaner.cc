// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"
#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

template <typename SegmentInfoT>
void segment_manager_info_t<SegmentInfoT>::init_segment_infos(
  SegmentInfoT&& segment_info)
{
  segment_infos.resize(num_segments, std::move(segment_info));
  for (device_segment_id_t j = 0; j < num_segments; j++) {
    segment_infos[j].segment = segment_id_t(device_id, j);
    crimson::get_logger(ceph_subsys_seastore).debug("added segment: {}", segment_id_t(device_id, j));
    if constexpr (std::is_same_v<SegmentInfoT, segment_info_t>) {
      segment_infos[j].smit = this;
    }
  }
}

void segment_info_t::set_open() {
  assert(state == Segment::segment_state_t::EMPTY);
  state = Segment::segment_state_t::OPEN;
  written_to = 0;
  set_unavail();
}

void segment_info_t::set_empty() {
  assert(state == Segment::segment_state_t::CLOSED);
  state = Segment::segment_state_t::EMPTY;
  written_to = 0;
  smit->usis.erase(usii);
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
    assert(i->segment == j->segment);
    if (i->live_bytes != j->live_bytes) {
      all_match = false;
      logger().debug(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->segment,
	i->live_bytes,
	j->live_bytes);
    }
  }
  return all_match;
}

int64_t SpaceTrackerDetailed::SegmentMap::allocate(
  device_segment_id_t segment,
  segment_off_t offset,
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
  segment_off_t offset,
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
    assert(i->segment == j->segment);
    if (i->segment_map.get_usage() != j->segment_map.get_usage()) {
      all_match = false;
      logger().error(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->segment,
	i->segment_map.get_usage(),
	j->segment_map.get_usage());
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
  segment_usage[id].segment_map.dump_usage(
    segment_usage[id.device_id()]->block_size);
}

SegmentCleaner::SegmentCleaner(
  config_t config,
  ExtentReaderRef&& scr,
  bool detailed)
  : detailed(detailed),
    config(config),
    scanner(std::move(scr)),
    gc_process(*this)
{
  register_metrics();
}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("segment_cleaner", {
    sm::make_counter("segments_released", stats.segments_released,
		     sm::description("total number of extents released by SegmentCleaner")),
  });
}

SegmentCleaner::get_segment_ret SegmentCleaner::get_segment(device_id_t id)
{
  for (auto it = segments.find_begin(id);
       it != segments.find_end(id);
       it++) {
    auto& segment_info = *it;
     if (segment_info.is_empty()) {
      mark_open(segment_info.segment);
      logger().debug("{}: returning segment {}", __func__, segment_info.segment);
      return get_segment_ret(
	get_segment_ertr::ready_future_marker{},
	segment_info.segment);
    }
  }
  assert(0 == "out of space handling todo");
  return get_segment_ret(
    get_segment_ertr::ready_future_marker{},
    0);
}

void SegmentCleaner::update_journal_tail_target(journal_seq_t target)
{
  logger().debug(
    "{}: {}, current tail target {}",
    __func__,
    target,
    journal_tail_target);
  assert(journal_tail_target == journal_seq_t() || target >= journal_tail_target);
  if (journal_tail_target == journal_seq_t() || target > journal_tail_target) {
    journal_tail_target = target;
  }
  gc_process.maybe_wake_on_space_used();
  maybe_wake_gc_blocked_io();
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  if (journal_tail_committed == journal_seq_t() ||
      committed > journal_tail_committed) {
    logger().debug(
      "{}: update journal_tail_committed {}",
      __func__,
      committed);
    journal_tail_committed = committed;
  }
  if (journal_tail_target == journal_seq_t() ||
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
  mark_closed(segment);
}

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  LOG_PREFIX(SegmentCleaner::rewrite_dirty);
  return ecb->get_next_dirty_extents(
    t,
    limit,
    config.journal_rewrite_per_cycle
  ).si_then([=, &t](auto dirty_list) {
    return seastar::do_with(
      std::move(dirty_list),
      [FNAME, this, &t](auto &dirty_list) {
	return trans_intr::do_for_each(
	  dirty_list,
	  [FNAME, this, &t](auto &e) {
	    DEBUGT("cleaning {}", t, *e);
	    return ecb->rewrite_extent(t, e);
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
        Transaction::src_t::CLEANER, [this](auto& t) {
      return rewrite_dirty(t, get_dirty_tail()
      ).si_then([this, &t] {
        return ecb->submit_transaction_direct(t);
      });
    });
  });
}

SegmentCleaner::gc_reclaim_space_ret SegmentCleaner::gc_reclaim_space()
{
  if (!scan_cursor) {
    paddr_t next = P_ADDR_NULL;
    next.segment = get_next_gc_target();
    if (next == P_ADDR_NULL) {
      logger().debug(
	"SegmentCleaner::do_gc: no segments to gc");
      return seastar::now();
    }
    next.offset = 0;
    scan_cursor =
      std::make_unique<ExtentReader::scan_extents_cursor>(
	next);
    logger().debug(
      "SegmentCleaner::do_gc: starting gc on segment {}",
      scan_cursor->get_offset().segment);
  } else {
    ceph_assert(!scan_cursor->is_complete());
  }

  return scanner->scan_extents(
    *scan_cursor,
    config.reclaim_bytes_stride
  ).safe_then([this](auto &&_extents) {
    return seastar::do_with(
        std::move(_extents),
        [this](auto &extents) {
      return repeat_eagain([this, &extents]() mutable {
        logger().debug(
          "SegmentCleaner::gc_reclaim_space: processing {} extents",
          extents.size());
        return ecb->with_transaction_intr(
            Transaction::src_t::CLEANER,
            [this, &extents](auto& t) {
          return trans_intr::do_for_each(
              extents,
              [this, &t](auto &extent) {
            auto &[addr, info] = extent;
            logger().debug(
              "SegmentCleaner::gc_reclaim_space: checking extent {}",
              info);
            return ecb->get_extent_if_live(
              t,
              info.type,
              addr,
              info.addr,
              info.len
            ).si_then([addr=addr, &t, this](CachedExtentRef ext) {
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
                return ecb->rewrite_extent(
                  t,
                  ext);
              }
            });
          }).si_then([this, &t] {
            if (scan_cursor->is_complete()) {
              t.mark_segment_to_release(scan_cursor->get_offset().segment);
            }
            return ecb->submit_transaction_direct(t);
          });
        });
      });
    });
  }).safe_then([this] {
    if (scan_cursor->is_complete()) {
      scan_cursor.reset();
    }
  });
}

SegmentCleaner::init_segments_ret SegmentCleaner::init_segments() {
  logger().debug("SegmentCleaner::init_segments: {} segments", segments.size());
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this](auto& segment_set) {
    return crimson::do_for_each(
      segments.begin(),
      segments.end(),
      [this, &segment_set](auto& segment_info) {
      auto segment_id = segment_info.segment;
      return scanner->read_segment_header(segment_id)
      .safe_then([&segment_set, segment_id, this](auto header) {
	if (header.out_of_line) {
	  logger().debug("ExtentReader::init_segments: out-of-line segment {}", segment_id);
	  init_mark_segment_closed(
	    segment_id,
	    header.journal_segment_seq,
	    true);
	} else {
	  logger().debug("ExtentReader::init_segments: journal segment {}", segment_id);
	  segment_set.emplace_back(std::make_pair(segment_id, std::move(header)));
	}
	return seastar::now();
      }).handle_error(
	crimson::ct_error::enoent::handle([](auto) {
	  return init_segments_ertr::now();
	}),
	crimson::ct_error::enodata::handle([](auto) {
	  return init_segments_ertr::now();
	}),
	crimson::ct_error::input_output_error::pass_further{}
      );
    }).safe_then([&segment_set] {
      return seastar::make_ready_future<
	std::vector<std::pair<segment_id_t, segment_header_t>>>(
	  std::move(segment_set));
    });
  });
}

}
