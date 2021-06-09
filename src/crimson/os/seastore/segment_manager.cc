// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

uint16_t SegmentManager::get_segment_manager_id() {
  return segment_manager_id;
}

}
