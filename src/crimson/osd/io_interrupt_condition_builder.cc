// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/exception.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/io_interrupt_condition_builder.h"

namespace crimson::osd {

template <typename Errorator>
IOInterruptConditionBuilder::interrupt_condition<
  Errorator>::interrupt_condition(epoch_t e, OSD* osd)
  : e(e), osd(osd) {}

template <typename Errorator>
typename Errorator::template future<>
IOInterruptConditionBuilder::interrupt_condition<
  Errorator>::operator()() {
  if (e != osd->get_map()->get_epoch()) {
    return crimson::common::eactingchg::make();
  }
  if (osd->get_state().is_prestop()
      || osd->get_state().is_stopping()) {
    return crimson::common::esysshut::make();
  }
  return typename Errorator::template future<>();
}

template <typename Errorator>
IOInterruptConditionBuilder::interrupt_condition<Errorator>
IOInterruptConditionBuilder::get_condition() {
  return interrupt_condition<Errorator>(
      osd.get_map()->get_epoch(), &osd);
}

}; // namespace crimson::osd
