// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "crimson/common/exception.h"

namespace crimson::osd {

class PG;

class IOInterruptCondition {
public:
  IOInterruptCondition(PG* pg);

  epoch_t get_current_osdmap_epoch();

  bool is_stopping();

  template <typename future_t>
  std::pair<bool, std::optional<future_t>> may_interrupt() {
    if (e != get_current_osdmap_epoch()) {
      return std::make_pair<bool, std::optional<future_t>>(true, crimson::common::eactingchg::make());
    }
    if (is_stopping()) {
      return std::make_pair<bool, std::optional<future_t>>(true, crimson::common::esysshut::make());
    }
    return std::make_pair<bool, std::optional<future_t>>(false, {});
  }

private:
  PG* pg;
  epoch_t e;
};

} // namespace crimson::osd
