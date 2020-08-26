// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/io_interrupt_condition_builder.h"
#include "crimson/osd/pg.h"

namespace crimson::osd {

IOInterruptCondition::IOInterruptCondition(PG* pg)
  : pg(pg), e(pg->get_osdmap_epoch()) {}

epoch_t IOInterruptCondition::get_current_osdmap_epoch() {
  return pg->get_osdmap_epoch();
}

bool IOInterruptCondition::is_stopping() {
  return pg->is_stopping();
}

} // namespace crimson::osd
