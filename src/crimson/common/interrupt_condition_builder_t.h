// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/exception.h"

namespace crimson::osd {

class PG;

class PG_interrupt_condition_builder_t {
public:
  PG_interrupt_condition_builder_t(PG& pg);

  std::function<void()> get_condition();
private:
  PG& pg;
};

} // namespace crimson::osd
