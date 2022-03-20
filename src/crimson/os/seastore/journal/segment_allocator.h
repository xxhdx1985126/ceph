// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_seq_allocator.h"

namespace crimson::os::seastore {
  class SegmentProvider;
}

namespace crimson::os::seastore::journal {

/**
 * SegmentAllocator
 *
 * Maintain an available segment for writes.
 */
class SegmentAllocator {
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;

 public:
  SegmentAllocator(segment_type_t type,
                   SegmentProvider &sp,
                   SegmentManager &sm,
                   SegmentSeqAllocator &ssa);

  device_id_t get_device_id() const {
    return segment_manager.get_device_id();
  }

  seastore_off_t get_block_size() const {
    return segment_manager.get_block_size();
  }

  extent_len_t get_max_write_length() const {
    return segment_manager.get_segment_size() -
           p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
                   size_t(segment_manager.get_block_size()));
  }

  device_segment_id_t get_num_segments() const {
    return segment_manager.get_num_segments();
  }

  bool can_write() const {
    return !!current_segment;
  }

  segment_id_t get_segment_id() const {
    assert(can_write());
    return current_segment->get_segment_id();
  }

  segment_nonce_t get_nonce() const {
    assert(can_write());
    return current_segment_nonce;
  }

  seastore_off_t get_written_to() const {
    assert(can_write());
    return written_to;
  }

  // returns true iff the current segment has insufficient space
  bool needs_roll(std::size_t length) const {
    assert(can_write());
    auto write_capacity = current_segment->get_write_capacity()
      - segment_manager.get_rounded_tail_length();
    return length + written_to > std::size_t(write_capacity);
  }

  // open for write
  using open_ertr = base_ertr;
  using open_ret = open_ertr::future<journal_seq_t>;
  open_ret open();

  // close the current segment and initialize next one
  using roll_ertr = base_ertr;
  roll_ertr::future<> roll();

  // write the buffer, return the write result
  //
  // May be called concurrently, but writes may complete in any order.
  // If rolling/opening, no write is allowed.
  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<write_result_t>;
  write_ret write(ceph::bufferlist to_write);

  using close_ertr = base_ertr;
  close_ertr::future<> close();

 private:
  void reset() {
    current_segment.reset();
    current_segment_nonce = 0;
    written_to = 0;
  }

  // FIXME: remove the unnecessary is_rolling
  using close_segment_ertr = base_ertr;
  close_segment_ertr::future<> close_segment(bool is_rolling);

  const segment_type_t type; // JOURNAL or OOL
  SegmentProvider &segment_provider;
  SegmentManager &segment_manager;
  SegmentRef current_segment;
  SegmentSeqAllocator &segment_seq_allocator;
  segment_nonce_t current_segment_nonce;
  seastore_off_t written_to;
};

}
