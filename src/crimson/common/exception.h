// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <tuple>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "crimson/common/errorator.h"
#include "crimson/common/log.h"

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

class system_shutdown_exception final : public std::exception {
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

using interruption_errorator = crimson::errorator<esysshut, eactingchg>;

template<bool may_loop = true, typename OpFunc, typename OnInterrupt>
inline seastar::future<> with_interruption(OpFunc&& func, OnInterrupt&& efunc)
{
  if constexpr (may_loop) {
    return crimson::do_until([func=std::move(func),
			     efunc=std::move(efunc)]
      () mutable -> interruption_errorator::future<bool> {
      return interruption_errorator::futurize<
		std::result_of_t<OpFunc()>>::apply(std::move(func), std::make_tuple())
      .handle_error(std::move(efunc));

    })._then([] { return seastar::now(); });
  } else {
    using trait = seastar::function_traits<OnInterrupt>;
    using ret_type = typename trait::return_type;
    return interruption_errorator::futurize<
	      std::result_of_t<OpFunc()>>::apply(std::move(func), std::make_tuple())
    .handle_error(std::move(efunc))
    ._then([](ret_type&&) {
	return interruption_errorator::now();
    });
  }
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

} // crimson::common

namespace std {
  template<>
  struct is_error_condition_enum<crimson::common::error> : public true_type {};
} // namespace std
