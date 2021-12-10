// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/interruptible_future.h"

namespace crimson::interruptible {
struct InterruptConditionPlaceHolder {
public:
  template <typename Fut>
  std::pair<bool, std::optional<Fut>> may_interrupt() {
    return {false, std::optional<Fut>()};
  }

  template <typename T>
  static constexpr bool is_interruption_v = false;

  static bool is_interruption(std::exception_ptr& eptr) {
    return false;
  }
};

template
thread_local interrupt_cond_t<InterruptConditionPlaceHolder>
interrupt_cond<InterruptConditionPlaceHolder>;

} // namespace crimson::interruptible
