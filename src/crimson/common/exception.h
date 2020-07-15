// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "crimson/common/log.h"

namespace crimson::common {

class system_shutdown_exception final : public std::exception{
public:
  const char* what() const noexcept final {
    return "system shutting down";
  }
};

class actingset_changed final : public std::exception {
public:
  actingset_changed(bool sp) : still_primary(sp) {}
  const char* what() const noexcept final {
    return "acting set changed";
  }
  bool is_primary() const {
    return still_primary;
  }
private:
  const bool still_primary;
};

template<typename OpFunc, typename OnInterrupt>
inline seastar::future<> with_interruption(bool may_loop, OpFunc&& func, OnInterrupt&& efunc)
{
  return [may_loop,
	  func=std::move(func),
	  efunc=std::move(efunc)]() mutable {
    if (may_loop) {
      return seastar::repeat([func=std::move(func),
			      efunc=std::move(efunc)]() mutable {
	return seastar::futurize_invoke(std::move(func))
		  .handle_exception(std::move(efunc));
      });
    } else {
	using trait = seastar::function_traits<OnInterrupt>;
	using ret_type = typename trait::return_type;
	return seastar::futurize_invoke(std::move(func))
	  .handle_exception(std::move(efunc))
	  .then([](ret_type&&) {
	    return seastar::now();
	  });
    }
  }().handle_exception_type([](crimson::common::system_shutdown_exception& e) {
      crimson::get_logger(ceph_subsys_osd).debug(
	  "operation skipped, system shutdown");
      return seastar::now();
  });
}

template<typename OpFunc>
inline seastar::future<> with_interruption(OpFunc&& func)
{
  return seastar::futurize_invoke(std::move(func)).handle_exception_type(
    [](crimson::common::system_shutdown_exception& e) {
    crimson::get_logger(ceph_subsys_osd).debug(
	"operation skipped, system shutdown");
    return seastar::now();
  });
}

}
