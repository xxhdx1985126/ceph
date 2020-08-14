// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"

namespace crimson::osd {

class OSD;

class IOInterruptConditionBuilder {
public:
  IOInterruptConditionBuilder(OSD& osd);

  template <typename Errorator>
  class interrupt_condition {
  public:
    interrupt_condition(epoch_t e, OSD& osd);

    typename Errorator::template future<> operator()();
  private:
    OSD& osd;
    epoch_t e;
  };

  template <typename Errorator>
  interrupt_condition<Errorator> get_condition();
private:
  OSD& osd;
};

} // namespace crimson::osd
