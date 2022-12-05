// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_epm);

namespace crimson::os::seastore {

SegmentedOolWriter::SegmentedOolWriter(
  data_category_t category,
  rewrite_gen_t gen,
  SegmentProvider& sp,
  SegmentSeqAllocator &ssa)
  : segment_allocator(nullptr, category, gen, sp, ssa),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     segment_allocator)
{
}

SegmentedOolWriter::alloc_write_ertr::future<>
SegmentedOolWriter::write_record(
  Transaction& t,
  record_t&& record,
  std::list<LogicalCachedExtentRef>&& extents)
{
  LOG_PREFIX(SegmentedOolWriter::write_record);
  assert(extents.size());
  assert(extents.size() == record.extents.size());
  assert(!record.deltas.size());

  // account transactional ool writes before write()
  auto& stats = t.get_ool_write_stats();
  stats.extents.num += extents.size();
  stats.extents.bytes += record.size.dlength;
  stats.md_bytes += record.size.get_raw_mdlength();
  stats.num_records += 1;

  return record_submitter.submit(std::move(record)
  ).safe_then([this, FNAME, &t, extents=std::move(extents)
              ](record_locator_t ret) mutable {
    DEBUGT("{} finish with {} and {} extents",
           t, segment_allocator.get_name(),
           ret, extents.size());
    paddr_t extent_addr = ret.record_block_base;
    for (auto& extent : extents) {
      TRACET("{} ool extent written at {} -- {}",
             t, segment_allocator.get_name(),
             extent_addr, *extent);
      t.mark_delayed_extent_ool(extent, extent_addr);
      extent_addr = extent_addr.as_seg_paddr().add_offset(
          extent->get_length());
    }
  });
}

SegmentedOolWriter::alloc_write_iertr::future<>
SegmentedOolWriter::do_write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  LOG_PREFIX(SegmentedOolWriter::do_write);
  assert(!extents.empty());
  if (!record_submitter.is_available()) {
    DEBUGT("{} extents={} wait ...",
           t, segment_allocator.get_name(),
           extents.size());
    return trans_intr::make_interruptible(
      record_submitter.wait_available()
    ).si_then([this, &t, &extents] {
      return do_write(t, extents);
    });
  }
  record_t record(TRANSACTION_TYPE_NULL);
  std::list<LogicalCachedExtentRef> pending_extents;
  auto commit_time = seastar::lowres_system_clock::now();

  for (auto it = extents.begin(); it != extents.end();) {
    auto& extent = *it;
    record_size_t wouldbe_rsize = record.size;
    wouldbe_rsize.account_extent(extent->get_bptr().length());
    using action_t = journal::RecordSubmitter::action_t;
    action_t action = record_submitter.check_action(wouldbe_rsize);
    if (action == action_t::ROLL) {
      auto num_extents = pending_extents.size();
      DEBUGT("{} extents={} submit {} extents and roll, unavailable ...",
             t, segment_allocator.get_name(),
             extents.size(), num_extents);
      auto fut_write = alloc_write_ertr::now();
      if (num_extents > 0) {
        assert(record_submitter.check_action(record.size) !=
               action_t::ROLL);
        fut_write = write_record(
            t, std::move(record), std::move(pending_extents));
      }
      return trans_intr::make_interruptible(
        record_submitter.roll_segment(
        ).safe_then([fut_write=std::move(fut_write)]() mutable {
          return std::move(fut_write);
        })
      ).si_then([this, &t, &extents] {
        return do_write(t, extents);
      });
    }

    TRACET("{} extents={} add extent to record -- {}",
           t, segment_allocator.get_name(),
           extents.size(), *extent);
    ceph::bufferlist bl;
    extent->prepare_write();
    bl.append(extent->get_bptr());
    assert(bl.length() == extent->get_length());
    auto modify_time = extent->get_modify_time();
    if (modify_time == NULL_TIME) {
      modify_time = commit_time;
    }
    record.push_back(
      extent_t{
        extent->get_type(),
        extent->get_laddr(),
        std::move(bl)},
      modify_time);
    pending_extents.push_back(extent);
    it = extents.erase(it);

    assert(record_submitter.check_action(record.size) == action);
    if (action == action_t::SUBMIT_FULL) {
      DEBUGT("{} extents={} submit {} extents ...",
             t, segment_allocator.get_name(),
             extents.size(), pending_extents.size());
      return trans_intr::make_interruptible(
        write_record(t, std::move(record), std::move(pending_extents))
      ).si_then([this, &t, &extents] {
        if (!extents.empty()) {
          return do_write(t, extents);
        } else {
          return alloc_write_iertr::now();
        }
      });
    }
    // SUBMIT_NOT_FULL: evaluate the next extent
  }

  auto num_extents = pending_extents.size();
  DEBUGT("{} submit the rest {} extents ...",
         t, segment_allocator.get_name(),
         num_extents);
  assert(num_extents > 0);
  return trans_intr::make_interruptible(
    write_record(t, std::move(record), std::move(pending_extents)));
}

SegmentedOolWriter::alloc_write_iertr::future<>
SegmentedOolWriter::alloc_write_ool_extents(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  if (extents.empty()) {
    return alloc_write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    return do_write(t, extents);
  });
}

void ExtentPlacementManager::init(
    JournalTrimmerImplRef &&trimmer, AsyncCleanerRef &&cleaner)
{
  writer_refs.clear();

  if (trimmer->get_journal_type() == journal_type_t::SEGMENTED) {
    auto segment_cleaner = dynamic_cast<SegmentCleaner*>(cleaner.get());
    ceph_assert(segment_cleaner != nullptr);
    auto num_writers = generation_to_writer(REWRITE_GENERATIONS);
    data_writers_by_gen.resize(num_writers, {});
    for (rewrite_gen_t gen = OOL_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
	    data_category_t::DATA, gen, *segment_cleaner,
	    segment_cleaner->get_ool_segment_seq_allocator()));
      data_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }

    md_writers_by_gen.resize(num_writers, {});
    for (rewrite_gen_t gen = OOL_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
	    data_category_t::METADATA, gen, *segment_cleaner,
	    segment_cleaner->get_ool_segment_seq_allocator()));
      md_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }

    for (auto *device : segment_cleaner->get_segment_manager_group()
				       ->get_segment_managers()) {
      add_device(device);
    }
  } else {
    assert(trimmer->get_journal_type() == journal_type_t::RANDOM_BLOCK);
    auto rb_cleaner = dynamic_cast<RBMCleaner*>(cleaner.get());
    ceph_assert(rb_cleaner != nullptr);
    auto num_writers = generation_to_writer(REWRITE_GENERATIONS);
    data_writers_by_gen.resize(num_writers, {});
    md_writers_by_gen.resize(num_writers, {});
    writer_refs.emplace_back(std::make_unique<RandomBlockOolWriter>(
	    rb_cleaner));
    // TODO: implement eviction in RBCleaner and introduce further writers
    data_writers_by_gen[generation_to_writer(OOL_GENERATION)] = writer_refs.back().get();
    md_writers_by_gen[generation_to_writer(OOL_GENERATION)] = writer_refs.back().get();
    for (auto *rb : rb_cleaner->get_rb_group()->get_rb_managers()) {
      add_device(rb->get_device());
    }
  }

  background_process.init(std::move(trimmer), std::move(cleaner));
}

void ExtentPlacementManager::set_primary_device(Device *device)
{
  ceph_assert(primary_device == nullptr);
  primary_device = device;
  if (device->get_backend_type() == backend_type_t::SEGMENTED) {
    prefer_ool = false;
  } else {
    ceph_assert(device->get_backend_type() == backend_type_t::RANDOM_BLOCK);
    prefer_ool = true;
  }
  ceph_assert(devices_by_id[device->get_device_id()] == device);
}

ExtentPlacementManager::open_ertr::future<>
ExtentPlacementManager::open_for_write()
{
  LOG_PREFIX(ExtentPlacementManager::open_for_write);
  INFO("started with {} devices", num_devices);
  ceph_assert(primary_device != nullptr);
  return crimson::do_for_each(data_writers_by_gen, [](auto &writer) {
    if (writer) {
      return writer->open();
    }
    return open_ertr::now();
  }).safe_then([this] {
    return crimson::do_for_each(md_writers_by_gen, [](auto &writer) {
      if (writer) {
	return writer->open();
      }
      return open_ertr::now();
    });
  });
}

ExtentPlacementManager::alloc_paddr_iertr::future<>
ExtentPlacementManager::delayed_allocate_and_write(
    Transaction &t,
    const std::list<LogicalCachedExtentRef> &delayed_extents)
{
  LOG_PREFIX(ExtentPlacementManager::delayed_allocate_and_write);
  DEBUGT("start with {} delayed extents",
         t, delayed_extents.size());
  assert(writer_refs.size());
  return seastar::do_with(
      std::map<ExtentOolWriter*, std::list<LogicalCachedExtentRef>>(),
      [this, &t, &delayed_extents](auto& alloc_map) {
    for (auto& extent : delayed_extents) {
      // For now, just do ool allocation for any delayed extent
      auto writer_ptr = get_writer(
          extent->get_user_hint(),
          get_extent_category(extent->get_type()),
          extent->get_rewrite_generation());
      alloc_map[writer_ptr].emplace_back(extent);
    }
    return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
      auto writer = p.first;
      auto& extents = p.second;
      return writer->alloc_write_ool_extents(t, extents);
    });
  });
}

ExtentPlacementManager::alloc_paddr_iertr::future<>
ExtentPlacementManager::write_preallocated_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> extents)
{
  LOG_PREFIX(ExtentPlacementManager::write_preallocated_ool_extents);
  DEBUGT("start with {} allocated extents",
         t, extents.size());
  assert(writer_refs.size());
  return seastar::do_with(
      std::map<ExtentOolWriter*, std::list<LogicalCachedExtentRef>>(),
      [this, &t, extents=std::move(extents)](auto& alloc_map) {
    for (auto& extent : extents) {
      auto writer_ptr = get_writer(
          extent->get_user_hint(),
          get_extent_category(extent->get_type()),
          extent->get_rewrite_generation());
      alloc_map[writer_ptr].emplace_back(extent);
    }
    return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
      auto writer = p.first;
      auto& extents = p.second;
      return writer->alloc_write_ool_extents(t, extents);
    });
  });
}

ExtentPlacementManager::close_ertr::future<>
ExtentPlacementManager::close()
{
  LOG_PREFIX(ExtentPlacementManager::close);
  INFO("started");
  return crimson::do_for_each(data_writers_by_gen, [](auto &writer) {
    if (writer) {
      return writer->close();
    }
    return close_ertr::now();
  }).safe_then([this] {
    return crimson::do_for_each(md_writers_by_gen, [](auto &writer) {
      if (writer) {
	return writer->close();
      }
      return close_ertr::now();
    });
  });
}

void ExtentPlacementManager::BackgroundProcess::log_state(const char *caller) const
{
  LOG_PREFIX(BackgroundProcess::log_state);
  DEBUG("caller {}, {}, {}",
        caller,
        JournalTrimmerImpl::stat_printer_t{*trimmer, true},
        AsyncCleaner::stat_printer_t{*cleaner, true});
}

void ExtentPlacementManager::BackgroundProcess::start_background()
{
  LOG_PREFIX(BackgroundProcess::start_background);
  INFO("{}, {}",
       JournalTrimmerImpl::stat_printer_t{*trimmer, true},
       AsyncCleaner::stat_printer_t{*cleaner, true});
  ceph_assert(trimmer->check_is_ready());
  ceph_assert(state == state_t::SCAN_SPACE);
  assert(!is_running());
  process_join = seastar::now();
  state = state_t::RUNNING;
  assert(is_running());
  process_join = run();
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::stop_background()
{
  return seastar::futurize_invoke([this] {
    if (!is_running()) {
      if (state != state_t::HALT) {
        state = state_t::STOP;
      }
      return seastar::now();
    }
    auto ret = std::move(*process_join);
    process_join.reset();
    state = state_t::HALT;
    assert(!is_running());
    do_wake_background();
    return ret;
  }).then([this] {
    LOG_PREFIX(BackgroundProcess::stop_background);
    INFO("done, {}, {}",
         JournalTrimmerImpl::stat_printer_t{*trimmer, true},
         AsyncCleaner::stat_printer_t{*cleaner, true});
    // run_until_halt() can be called at HALT
  });
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::run_until_halt()
{
  ceph_assert(state == state_t::HALT);
  assert(!is_running());
  if (is_running_until_halt) {
    return seastar::now();
  }
  is_running_until_halt = true;
  return seastar::do_until(
    [this] {
      log_state("run_until_halt");
      assert(is_running_until_halt);
      if (background_should_run()) {
        return false;
      } else {
        is_running_until_halt = false;
        return true;
      }
    },
    [this] {
      return do_background_cycle();
    }
  );
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::reserve_projected_usage(
    std::size_t projected_usage)
{
  ceph_assert(is_ready());
  ceph_assert(!blocking_io);
  // The pipeline configuration prevents another IO from entering
  // prepare until the prior one exits and clears this.
  ++stats.io_count;
  bool is_blocked = false;
  if (trimmer->should_block_io_on_trim()) {
    is_blocked = true;
    ++stats.io_blocked_count_trim;
  }
  if (cleaner->should_block_io_on_clean()) {
    is_blocked = true;
    ++stats.io_blocked_count_clean;
  }
  if (is_blocked) {
    ++stats.io_blocking_num;
    ++stats.io_blocked_count;
    stats.io_blocked_sum += stats.io_blocking_num;
  }
  return seastar::do_until(
    [this] {
      log_state("reserve_projected_usage(await_hard_limits)");
      return !should_block_io();
    },
    [this] {
      blocking_io = seastar::promise<>();
      if (time_point_block.time_since_epoch() == std::chrono::steady_clock::duration()) {
        time_point_block = std::chrono::steady_clock::now();
      }
      return blocking_io->get_future();
    }
  ).then([this, is_blocked, projected_usage] {
    ceph_assert(!blocking_io);
    if (is_blocked) {
      assert(stats.io_blocking_num > 0);
      --stats.io_blocking_num;
    }
    cleaner->reserve_projected_usage(projected_usage);
  });
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::run()
{
  assert(is_running());
  return seastar::repeat([this] {
    if (!is_running()) {
      log_state("run(exit)");
      return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
    }
    return seastar::futurize_invoke([this] {
      if (background_should_run()) {
        log_state("run(background)");
        return do_background_cycle();
      } else {
        log_state("run(block)");
        ceph_assert(!blocking_background);
        blocking_background = seastar::promise<>();
        return blocking_background->get_future();
      }
    }).then([] {
      return seastar::stop_iteration::no;
    });
  });
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::do_background_cycle()
{
  assert(is_ready());
  if (trimmer->should_trim_alloc()) {
    return trimmer->trim_alloc(
    ).safe_then([this] {
      if (time_point_block.time_since_epoch() !=
          std::chrono::steady_clock::duration()) {
        auto now = std::chrono::steady_clock::now();
        auto d = now - time_point_block;
        stats.time_blocked_on_trim_alloc += d.count();
        time_point_block = now;
      }
    }).handle_error(
      crimson::ct_error::assert_all{
	"do_background_cycle encountered invalid error in trim_alloc"
      }
    );
  } else if (trimmer->should_trim_dirty()) {
    return trimmer->trim_dirty(
    ).safe_then([this] {
      if (time_point_block.time_since_epoch() !=
          std::chrono::steady_clock::duration()) {
        auto now = std::chrono::steady_clock::now();
        auto d = now - time_point_block;
        stats.time_blocked_on_trim_dirty += d.count();
        time_point_block = now;
      }
    }).handle_error(
      crimson::ct_error::assert_all{
	"do_background_cycle encountered invalid error in trim_dirty"
      }
    );
  } else if (cleaner->should_clean_space()) {
    return cleaner->clean_space(
    ).safe_then([this] {
      if (time_point_block.time_since_epoch() !=
          std::chrono::steady_clock::duration()) {
        auto now = std::chrono::steady_clock::now();
        auto d = now - time_point_block;
        stats.time_blocked_on_clean += d.count();
        time_point_block = now;
      }
    }).handle_error(
      crimson::ct_error::assert_all{
	"do_background_cycle encountered invalid error in clean_space"
      }
    );
  } else {
    return seastar::now();
  }
}

void ExtentPlacementManager::BackgroundProcess::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("background_process", {
    sm::make_counter("io_count", stats.io_count,
                     sm::description("the sum of IOs")),
    sm::make_counter("io_blocked_count", stats.io_blocked_count,
                     sm::description("IOs that are blocked by gc")),
    sm::make_counter("io_blocked_count_trim", stats.io_blocked_count_trim,
                     sm::description("IOs that are blocked by trimming")),
    sm::make_counter("io_blocked_count_clean", stats.io_blocked_count_clean,
                     sm::description("IOs that are blocked by cleaning")),
    sm::make_counter("io_blocked_sum", stats.io_blocked_sum,
                     sm::description("the sum of blocking IOs")),

    sm::make_counter("time_blocked_on_trim_alloc", stats.time_blocked_on_trim_alloc,
                     sm::description("total time of io being blocked by trim_alloc")),
    sm::make_counter("time_blocked_on_trim_dirty", stats.time_blocked_on_trim_dirty,
                     sm::description("total time of io being blocked by trim_dirty")),
    sm::make_counter("time_blocked_on_clean", stats.time_blocked_on_clean,
                     sm::description("total time of io being blocked"))
  });
}

RandomBlockOolWriter::alloc_write_iertr::future<>
RandomBlockOolWriter::alloc_write_ool_extents(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  if (extents.empty()) {
    return alloc_write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    return do_write(t, extents);
  });
}

RandomBlockOolWriter::alloc_write_iertr::future<>
RandomBlockOolWriter::do_write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  LOG_PREFIX(RandomBlockOolWriter::do_write);
  assert(!extents.empty());
  DEBUGT("start with {} allocated extents",
         t, extents.size());
  return trans_intr::do_for_each(extents,
    [this, &t, FNAME](auto& ex) {
    auto paddr = ex->get_paddr();
    assert(paddr.is_absolute());
    RandomBlockManager * rbm = rb_cleaner->get_rbm(paddr); 
    assert(rbm);
    TRACE("extent {}, allocated addr {}", ex, paddr);
    auto& stats = t.get_ool_write_stats();
    stats.extents.num += 1;
    stats.extents.bytes += ex->get_length();
    stats.num_records += 1;

    return rbm->write(paddr,
      ex->get_bptr()
    ).handle_error(
      alloc_write_iertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error when writing record"}
    ).safe_then([&t, &ex, paddr, FNAME]() {
      TRACET("ool extent written at {} -- {}",
	     t, paddr, *ex);
      t.mark_allocated_extent_ool(ex);
      return alloc_write_iertr::now();
    });
  });
}

}
