// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "crimson/common/exception.h"

namespace crimson::osd {

class OSD;

class IOInterruptConditionBuilder {
public:
  static void set_osd(OSD* osd) {
    IOInterruptConditionBuilder::osd = osd;
  }

  static epoch_t get_osdmap_epoch();

  static bool is_osd_prestop();
  static bool is_osd_stopping();

  template <typename Errorator>
  class interrupt_condition {
  public:
    interrupt_condition()
      : e(IOInterruptConditionBuilder::get_osdmap_epoch()) {}

    typename Errorator::template future<> operator()() {
      if (e != IOInterruptConditionBuilder::get_osdmap_epoch()) {
	return crimson::common::eactingchg::make();
      }
      if (IOInterruptConditionBuilder::is_osd_prestop()
	  || IOInterruptConditionBuilder::is_osd_stopping()) {
	return crimson::common::esysshut::make();
      }
      return typename Errorator::template future<>();
    }
    
    static interrupt_condition<Errorator> get_condition() {
      return interrupt_condition<Errorator>();
    }
  private:
    epoch_t e;
  };

private:
  static OSD* osd;
};

} // namespace crimson::osd
