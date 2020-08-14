// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "crimson/common/exception.h"

namespace crimson::osd {

class PG;

class IOInterruptConditionBuilder {
public:
  IOInterruptConditionBuilder(PG* pg)
    : pg(pg) {}

  epoch_t get_osdmap_epoch();

  bool is_stopping();

  template <typename Errorator>
  class interrupt_condition {
  public:
    interrupt_condition(IOInterruptConditionBuilder* builder)
      : e(builder->get_osdmap_epoch()), builder(builder) {}

    typename Errorator::template future<> operator()() {
      if (e != builder->get_osdmap_epoch()) {
	return crimson::common::eactingchg::make();
      }
      if (builder->is_stopping()) {
	return crimson::common::esysshut::make();
      }
      return typename Errorator::template future<>();
    }
  private:
    epoch_t e;
    IOInterruptConditionBuilder* builder;
  };

  template <typename Errorator>
  using interrupt_condition_ref = std::unique_ptr<interrupt_condition<Errorator>>;

  template <typename Errorator>
  interrupt_condition_ref<Errorator> get_condition() {
    return std::make_unique<interrupt_condition<Errorator>>(this);
  }

private:
  PG* pg;
};

} // namespace crimson::osd
