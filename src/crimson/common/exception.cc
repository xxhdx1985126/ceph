// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/exception.h"

namespace crimson::common {

const std::error_category& crimson_category()
{
  struct category : public std::error_category {
    const char* name() const noexcept override {
      return "crimson::common";
    }

    std::string message(int ev) const override {
      switch (static_cast<error>(ev)) {
        case error::success:
          return "success";
	case error::system_shutdown:
	  return "system shutdown";
	case error::actingset_change:
	  return "actingset change";
        default:
          return "unknown";
      }
    }
  };
  static category instance;
  return instance;
}

} // namespace crimson::common
