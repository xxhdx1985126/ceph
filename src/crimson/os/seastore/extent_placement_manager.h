// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "seastar/core/gate.hh"

#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal/segment_allocator.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"
#include "crimson/os/seastore/randomblock_manager_group.h"

namespace crimson::os::seastore {

/**
 * ExtentOolWriter
 *
 * Write the extents as out-of-line and allocate the physical addresses.
 * Different writers write extents to different locations.
 */
class ExtentOolWriter {
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
public:
  virtual ~ExtentOolWriter() {}

  using open_ertr = base_ertr;
  virtual open_ertr::future<> open() = 0;

  virtual paddr_t alloc_paddr(extent_len_t length) = 0;

  using alloc_write_ertr = base_ertr;
  using alloc_write_iertr = trans_iertr<alloc_write_ertr>;
  virtual alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> &extents) = 0;

  using close_ertr = base_ertr;
  virtual close_ertr::future<> close() = 0;
};
using ExtentOolWriterRef = std::unique_ptr<ExtentOolWriter>;

/**
 * SegmentedOolWriter
 *
 * Different writers write extents to different out-of-line segments provided
 * by the SegmentProvider.
 */
class SegmentedOolWriter : public ExtentOolWriter {
public:
  SegmentedOolWriter(data_category_t category,
                     rewrite_gen_t gen,
                     SegmentProvider &sp,
                     SegmentSeqAllocator &ssa);

  open_ertr::future<> open() final {
    return record_submitter.open(false).discard_result();
  }

  alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> &extents) final;

  close_ertr::future<> close() final {
    return write_guard.close().then([this] {
      return record_submitter.close();
    }).safe_then([this] {
      write_guard = seastar::gate();
    });
  }

  paddr_t alloc_paddr(extent_len_t length) final {
    return make_delayed_temp_paddr(0);
  }

private:
  alloc_write_iertr::future<> do_write(
    Transaction& t,
    std::list<LogicalCachedExtentRef> &extent);

  alloc_write_ertr::future<> write_record(
    Transaction& t,
    record_t&& record,
    std::list<LogicalCachedExtentRef> &&extents);

  journal::SegmentAllocator segment_allocator;
  journal::RecordSubmitter record_submitter;
  seastar::gate write_guard;
};


class RandomBlockOolWriter : public ExtentOolWriter {
public:
  RandomBlockOolWriter(RBMCleaner* rb_cleaner) :
    rb_cleaner(rb_cleaner) {}

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open() final {
    return open_ertr::now();
  }

  alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> &extents) final;

  close_ertr::future<> close() final {
    return write_guard.close().then([this] {
      write_guard = seastar::gate();
      return close_ertr::now();
    });
  }

  paddr_t alloc_paddr(extent_len_t length) final {
    assert(rb_cleaner);
    return rb_cleaner->alloc_paddr(length);
  }

private:
  alloc_write_iertr::future<> do_write(
    Transaction& t,
    std::list<LogicalCachedExtentRef> &extent);

  RBMCleaner* rb_cleaner;
  seastar::gate write_guard;
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager()
    : ool_segment_seq_allocator(
          std::make_unique<SegmentSeqAllocator>(segment_type_t::OOL))
  {
    devices_by_id.resize(DEVICE_ID_MAX, nullptr);
  }

  void init(JournalTrimmerImplRef &&, AsyncCleanerRef &&);

  SegmentSeqAllocator* get_ool_segment_seq_allocator() const {
    return ool_segment_seq_allocator.get();
  }

  void set_primary_device(Device *device);

  void set_extent_callback(ExtentCallbackInterface *cb) {
    background_process.set_extent_callback(cb);
  }

  journal_type_t get_journal_type() const {
    return background_process.get_journal_type();
  }

  extent_len_t get_block_size() const {
    assert(primary_device != nullptr);
    // assume all the devices have the same block size
    return primary_device->get_block_size();
  }

  Device& get_primary_device() {
    assert(primary_device != nullptr);
    return *primary_device;
  }

  store_statfs_t get_stat() const {
    return background_process.get_stat();
  }

  using mount_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
  using mount_ret = mount_ertr::future<>;
  mount_ret mount() {
    return background_process.mount();
  }

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open_for_write();

  void start_scan_space() {
    return background_process.start_scan_space();
  }

  void start_background() {
    return background_process.start_background();
  }

  struct alloc_result_t {
    paddr_t paddr;
    bufferptr bp;
    rewrite_gen_t gen;
  };
  alloc_result_t alloc_new_extent(
    Transaction& t,
    extent_types_t type,
    extent_len_t length,
    placement_hint_t hint,
    rewrite_gen_t gen
  ) {
    assert(hint < placement_hint_t::NUM_HINTS);
    assert(is_target_rewrite_generation(gen));
    assert(gen == INIT_GENERATION || hint == placement_hint_t::REWRITE);

    // XXX: bp might be extended to point to different memory (e.g. PMem)
    // according to the allocator.
    auto alloc_paddr = [this](rewrite_gen_t gen, 
      data_category_t category, extent_len_t length) 
      -> alloc_result_t {
      auto bp = ceph::bufferptr(
      buffer::create_page_aligned(length));
      bp.zero();
      paddr_t addr;
      if (gen == INLINE_GENERATION) {
	addr = make_record_relative_paddr(0);
      } else if (category == data_category_t::DATA) {
	assert(data_writers_by_gen[generation_to_writer(gen)]);
	addr = data_writers_by_gen[
	  generation_to_writer(gen)]->alloc_paddr(length);
      } else {
	assert(category == data_category_t::METADATA);
	assert(md_writers_by_gen[generation_to_writer(gen)]);
	addr = md_writers_by_gen[
	  generation_to_writer(gen)]->alloc_paddr(length);
      }
      return {addr,
	      std::move(bp),
	      gen};
    };

    if (!is_logical_type(type)) {
      // TODO: implement out-of-line strategy for physical extent.
      assert(get_extent_category(type) == data_category_t::METADATA);
      return alloc_paddr(INLINE_GENERATION, data_category_t::METADATA, length);
    }

    if (hint == placement_hint_t::COLD) {
      assert(gen == INIT_GENERATION);
      return alloc_paddr(MIN_REWRITE_GENERATION, get_extent_category(type), length);
    }

    if (get_extent_category(type) == data_category_t::METADATA &&
        gen == INIT_GENERATION) {
      if (prefer_ool) {
	return alloc_paddr(OOL_GENERATION, get_extent_category(type), length);
      } else {
        // default not to ool metadata extents to reduce padding overhead.
        // TODO: improve padding so we can default to the prefer_ool path.
	return alloc_paddr(INLINE_GENERATION, get_extent_category(type), length);
      }
    } else {
      assert(get_extent_category(type) == data_category_t::DATA ||
             gen >= MIN_REWRITE_GENERATION);
      if (gen > MAX_REWRITE_GENERATION) {
        gen = MAX_REWRITE_GENERATION;
      } else if (gen == INIT_GENERATION) {
        gen = OOL_GENERATION;
      }
      return alloc_paddr(gen, get_extent_category(type), length);
    }
  }

  /**
   * dispatch_result_t
   *
   * ool extents are placed in alloc_map and passed to
   * EPM::write_delayed_ool_extents,
   * delayed_extents is used to update lba mapping.
   * usage is used to reserve projected space
   */
  struct projected_usage_t {
    std::size_t inline_usage = 0;
    std::size_t ool_usage = 0;
  };
  using extents_by_writer_t =
    std::map<ExtentOolWriter*, std::list<LogicalCachedExtentRef>>;
  struct dispatch_result_t {
    extents_by_writer_t alloc_map;
    std::list<LogicalCachedExtentRef> delayed_extents;
    projected_usage_t usage;
  };

  /**
   * dispatch_delayed_extents
   *
   * Performs delayed allocation
   */
  dispatch_result_t dispatch_delayed_extents(Transaction& t);

  /**
   * write_delayed_ool_extents
   *
   * Do writes for out-of-line extents.
   */
  using alloc_paddr_iertr = ExtentOolWriter::alloc_write_iertr;
  alloc_paddr_iertr::future<> write_delayed_ool_extents(
    Transaction& t,
    extents_by_writer_t& alloc_map);

  /**
   * write_preallocated_ool_extents
   *
   * Performs ool writes for extents with pre-allocated addresses.
   * See Transaction::pre_alloc_list
   */
  alloc_paddr_iertr::future<> write_preallocated_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> extents);

  seastar::future<> stop_background() {
    return background_process.stop_background();
  }

  using close_ertr = ExtentOolWriter::close_ertr;
  close_ertr::future<> close();

  using read_ertr = Device::read_ertr;
  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out
  ) {
    assert(devices_by_id[addr.get_device_id()] != nullptr);
    return devices_by_id[addr.get_device_id()]->read(addr, len, out);
  }

  void mark_space_used(paddr_t addr, extent_len_t len) {
    background_process.mark_space_used(addr, len);
  }

  void mark_space_free(paddr_t addr, extent_len_t len) {
    background_process.mark_space_free(addr, len);
  }

  void commit_space_used(paddr_t addr, extent_len_t len) {
    return background_process.commit_space_used(addr, len);
  }

  seastar::future<> reserve_projected_usage(projected_usage_t usage) {
    return background_process.reserve_projected_usage(usage);
  }

  void release_projected_usage(projected_usage_t usage) {
    background_process.release_projected_usage(usage);
  }

  // Testing interfaces

  void test_init_no_background(Device *test_device) {
    assert(test_device->get_backend_type() == backend_type_t::SEGMENTED);
    add_device(test_device);
    set_primary_device(test_device);
  }

  bool check_usage() {
    return background_process.check_usage();
  }

  seastar::future<> run_background_work_until_halt() {
    return background_process.run_until_halt();
  }

private:
  void add_device(Device *device) {
    auto device_id = device->get_device_id();
    ceph_assert(devices_by_id[device_id] == nullptr);
    devices_by_id[device_id] = device;
    ++num_devices;
  }

  /**
   * dispatch_delayed_extent
   *
   * Specify the extent inline or ool
   * return true indicates inline otherwise ool
   */
  bool dispatch_delayed_extent(LogicalCachedExtentRef& extent) {
    // TODO: all delayed extents are ool currently
    boost::ignore_unused(extent);
    return false;
  }

  ExtentOolWriter* get_writer(placement_hint_t hint,
                              data_category_t category,
                              rewrite_gen_t gen) {
    assert(hint < placement_hint_t::NUM_HINTS);
    assert(is_rewrite_generation(gen));
    assert(gen != INLINE_GENERATION);
    if (category == data_category_t::DATA) {
      return data_writers_by_gen[generation_to_writer(gen)];
    } else {
      assert(category == data_category_t::METADATA);
      return md_writers_by_gen[generation_to_writer(gen)];
    }
  }

  /**
   * BackgroundProcess
   *
   * Background process to schedule background transactions.
   *
   * TODO: device tiering
   */
  class BackgroundProcess : public BackgroundListener {
  public:
    BackgroundProcess() = default;

    void init(JournalTrimmerImplRef &&_trimmer,
              AsyncCleanerRef &&_cleaner) {
      trimmer = std::move(_trimmer);
      trimmer->set_background_callback(this);
      major_cleaner = std::move(_cleaner);
      major_cleaner->set_background_callback(this);
    }

    journal_type_t get_journal_type() const {
      return trimmer->get_journal_type();
    }

    void set_extent_callback(ExtentCallbackInterface *cb) {
      trimmer->set_extent_callback(cb);
      major_cleaner->set_extent_callback(cb);
    }

    store_statfs_t get_stat() const {
      return major_cleaner->get_stat();
    }

    using mount_ret = ExtentPlacementManager::mount_ret;
    mount_ret mount() {
      ceph_assert(state == state_t::STOP);
      state = state_t::MOUNT;
      trimmer->reset();
      stats = {};
      register_metrics();
      return major_cleaner->mount();
    }

    void start_scan_space() {
      ceph_assert(state == state_t::MOUNT);
      state = state_t::SCAN_SPACE;
      ceph_assert(major_cleaner->check_usage_is_empty());
    }

    void start_background();

    void mark_space_used(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }
      assert(major_cleaner);
      major_cleaner->mark_space_used(addr, len);
    }

    void mark_space_free(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }
      assert(major_cleaner);
      major_cleaner->mark_space_free(addr, len);
    }

    void commit_space_used(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }
      assert(major_cleaner);
      return major_cleaner->commit_space_used(addr, len);
    }

    seastar::future<> reserve_projected_usage(projected_usage_t usage);

    void release_projected_usage(projected_usage_t usage) {
      if (is_ready()) {
        trimmer->release_inline_usage(usage.inline_usage);
        major_cleaner->release_projected_usage(usage.inline_usage + usage.ool_usage);
      }
    }

    seastar::future<> stop_background();

    // Testing interfaces

    bool check_usage() {
      return major_cleaner->check_usage();
    }

    seastar::future<> run_until_halt();

  protected:
    state_t get_state() const final {
      return state;
    }

    void maybe_wake_background() final {
      if (!is_running()) {
        return;
      }
      if (background_should_run()) {
        do_wake_background();
      }
    }

    void maybe_wake_blocked_io() final {
      if (!is_ready()) {
        return;
      }
      if (!should_block_io() && blocking_io) {
        blocking_io->set_value();
        blocking_io = std::nullopt;
      }
    }

  private:
    bool is_running() const {
      if (state == state_t::RUNNING) {
        assert(process_join);
        return true;
      } else {
        assert(!process_join);
        return false;
      }
    }

    void log_state(const char *caller) const;

    seastar::future<> run();

    void do_wake_background() {
      if (blocking_background) {
	blocking_background->set_value();
	blocking_background = std::nullopt;
      }
    }

    bool background_should_run() const {
      assert(is_ready());
      return major_cleaner->should_clean_space()
        || trimmer->should_trim_dirty()
        || trimmer->should_trim_alloc();
    }

    bool should_block_io() const {
      assert(is_ready());
      return trimmer->should_block_io_on_trim() ||
             major_cleaner->should_block_io_on_clean();
    }

    struct reserve_result_t {
      bool reserve_inline_success = true;
      bool reserve_ool_success = true;

      bool is_successful() const {
        return reserve_inline_success && reserve_ool_success;
      }
    };

    reserve_result_t try_reserve(const projected_usage_t &usage);

    seastar::future<> do_background_cycle();

    void register_metrics();

    struct {
      uint64_t io_blocking_num = 0;
      uint64_t io_count = 0;
      uint64_t io_blocked_count = 0;
      uint64_t io_blocked_count_trim = 0;
      uint64_t io_blocked_count_clean = 0;
      uint64_t io_blocked_sum = 0;
    } stats;
    seastar::metrics::metric_group metrics;

    JournalTrimmerImplRef trimmer;
    AsyncCleanerRef major_cleaner;

    std::optional<seastar::future<>> process_join;
    std::optional<seastar::promise<>> blocking_background;
    std::optional<seastar::promise<>> blocking_io;
    bool is_running_until_halt = false;
    state_t state = state_t::STOP;
  };

  bool prefer_ool;
  std::vector<ExtentOolWriterRef> writer_refs;
  std::vector<ExtentOolWriter*> data_writers_by_gen;
  // gen 0 METADATA writer is the journal writer
  std::vector<ExtentOolWriter*> md_writers_by_gen;

  std::vector<Device*> devices_by_id;
  Device* primary_device = nullptr;
  std::size_t num_devices = 0;

  BackgroundProcess background_process;
  // TODO: drop once paddr->journal_seq_t is introduced
  SegmentSeqAllocatorRef ool_segment_seq_allocator;
};

using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

std::ostream &operator<<(std::ostream &, const ExtentPlacementManager::projected_usage_t &);

}

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<crimson::os::seastore::ExtentPlacementManager::projected_usage_t>
  : fmt::ostream_formatter {};
#endif
