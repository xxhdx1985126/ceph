// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/errorator.h"

namespace crimson::common {
// crimson common
enum class error {
  success = 0,
  system_shutdown,
  actingset_change,
};

const std::error_category& crimson_category();

inline std::error_code make_error_code(error e)
{
  return {static_cast<int>(e), crimson_category()};
}

template <error ErrorV>
inline std::error_code ec = make_error_code(ErrorV);

template <error ErrorV>
using crimson_error_code =
  crimson::unthrowable_wrapper<const std::error_code&, ec<ErrorV>>;

using esysshut = crimson_error_code<error::system_shutdown>;
using eactingchg = crimson_error_code<error::actingset_change>;
using esuccess = crimson_error_code<error::success>;

} // namespace crimson::common
