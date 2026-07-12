// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <cstdint>
#include <ostream>

#include "include/encoding.h"

struct backfill_reservation_space_info_t {
  static constexpr uint32_t RATIO_SCALE = 1000000;
  static constexpr uint32_t PRIORITY_SCALE = 100;

  uint32_t relieved_usage_before = 0;
  uint32_t relieved_usage_after = 0;
  uint32_t target_usage_before = 0;
  uint32_t target_usage_after = 0;

  static uint32_t encode_usage_ratio(double ratio) {
    if (ratio <= 0) {
      return 0;
    }
    if (ratio >= 1) {
      return RATIO_SCALE;
    }
    return static_cast<uint32_t>(ratio * RATIO_SCALE);
  }

  static uint32_t encode_usage_ratio(uint64_t used, uint64_t total) {
    if (total == 0) {
      return 0;
    }
    return encode_usage_ratio(static_cast<double>(used) / total);
  }

  int64_t raw_score() const {
    return static_cast<int64_t>(relieved_usage_before) -
      std::max<int64_t>(relieved_usage_after, target_usage_after);
  }

  unsigned priority_boost(unsigned priority_band_remaining) const {
    const int64_t score = raw_score();
    if (score <= 0) {
      return 0;
    }
    const uint64_t scaled = static_cast<uint64_t>(score) * PRIORITY_SCALE;
    const uint64_t boost = scaled / RATIO_SCALE;
    return std::min<uint64_t>(boost, priority_band_remaining);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(relieved_usage_before, bl);
    ceph::encode(relieved_usage_after, bl);
    ceph::encode(target_usage_before, bl);
    ceph::encode(target_usage_after, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(relieved_usage_before, p);
    ceph::decode(relieved_usage_after, p);
    ceph::decode(target_usage_before, p);
    ceph::decode(target_usage_after, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(backfill_reservation_space_info_t)

struct backfill_osd_space_usage_t {
  uint64_t used_bytes = 0;
  uint64_t total_bytes = 0;

  uint32_t usage_ratio() const {
    return backfill_reservation_space_info_t::encode_usage_ratio(
      used_bytes, total_bytes);
  }

  uint32_t projected_usage_ratio(int64_t delta_bytes) const {
    uint64_t projected = used_bytes;
    if (delta_bytes < 0) {
      const uint64_t reduction = static_cast<uint64_t>(-delta_bytes);
      projected = reduction > projected ? 0 : projected - reduction;
    } else if (delta_bytes > 0) {
      const uint64_t increase = static_cast<uint64_t>(delta_bytes);
      projected = total_bytes - projected < increase ?
	total_bytes : projected + increase;
    }
    return backfill_reservation_space_info_t::encode_usage_ratio(
      projected, total_bytes);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(used_bytes, bl);
    ceph::encode(total_bytes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(used_bytes, p);
    ceph::decode(total_bytes, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(backfill_osd_space_usage_t)

inline std::ostream& operator<<(
  std::ostream& out,
  const backfill_reservation_space_info_t& info)
{
  return out << "relieved " << info.relieved_usage_before
	     << "->" << info.relieved_usage_after
	     << " target " << info.target_usage_before
	     << "->" << info.target_usage_after
	     << " ppm";
}
