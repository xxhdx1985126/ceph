// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/exception.h"

namespace crimson::common {

class EmptyInterruptCondition {
public:
  template <typename future_t>
  std::pair<bool, future_t> may_interrupt() {
    return std::make_pair<bool, future_t>(false, crimson::common::esuccess::make());
  }
};

} // namespace crimson::common
