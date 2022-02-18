// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/backref/btree_backref_manager.h"

namespace crimson::os::seastore::backref {

BackrefManagerRef create_backref_manager(
  SegmentManager &segment_manager,
  Cache &cache)
{
  return BackrefManagerRef(
    new BtreeBackrefManager(
      segment_manager, cache));
}

} // namespace crimson::os::seastore::backref

