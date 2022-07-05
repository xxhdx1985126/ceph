// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "transaction.h"
#include "crimson/common/interruptible_future.h"

namespace crimson::interruptible {
template
thread_local interrupt_cond_t<::crimson::os::seastore::TransactionConflictCondition>
interrupt_cond<::crimson::os::seastore::TransactionConflictCondition>;
}

namespace crimson::os::seastore {

void Transaction::set_reclaimed_to(paddr_t to) {
  assert(is_cleaner_transaction(src));
  reclaimed_to = to;
}

} // crimson::os::seastore
