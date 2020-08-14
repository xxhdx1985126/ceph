// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/io_interrupt_condition_builder.h"
#include "crimson/osd/osd.h"

namespace crimson::osd {

epoch_t IOInterruptConditionBuilder::get_osdmap_epoch() {
  return pg->get_osdmap_epoch();
}

bool IOInterruptConditionBuilder::is_stopping() {
  return pg->is_stopping();
}

} // namespace crimson::osd
