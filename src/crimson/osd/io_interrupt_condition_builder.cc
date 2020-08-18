// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/io_interrupt_condition_builder.h"
#include "crimson/osd/osd.h"

namespace crimson::osd {

OSD* IOInterruptConditionBuilder::osd = nullptr;

epoch_t IOInterruptConditionBuilder::get_osdmap_epoch() {
  return IOInterruptConditionBuilder::osd->get_map()->get_epoch();
}

bool IOInterruptConditionBuilder::is_osd_prestop() {
  return IOInterruptConditionBuilder::osd->get_state().is_prestop();
}

bool IOInterruptConditionBuilder::is_osd_stopping() {
  return IOInterruptConditionBuilder::osd->get_state().is_stopping();
}

} // namespace crimson::osd
