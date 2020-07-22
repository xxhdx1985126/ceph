// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_interruptible_condition_builder.h"
#include "pg.h"

namespace crimson::osd {

PG_interrupt_condition_builder_t::PG_interrupt_condition_builder_t(PG& pg)
  : pg(pg)
{}

std::function<void()> PG_interrupt_condition_builder_t::get_condition() {
  return [pg=&pg, e=pg.get_osdmap_epoch()] {
    if (e != pg->get_osdmap_epoch()) {
      throw crimson::common::actingset_changed(pg->is_primary());
    }
  };
}

} // namespace crimson::osd
