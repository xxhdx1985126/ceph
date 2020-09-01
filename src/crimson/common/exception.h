// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <exception>
#include <tuple>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

#include "crimson/common/error.h"
#include "crimson/common/log.h"

namespace crimson::common {

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

template <typename INT_COND_BUILDER>
using interruption_errorator = crimson::errorator<INT_COND_BUILDER,
						  crimson::interrupt_error_carrier<
						    esuccess,
						    esysshut,
						    eactingchg>>;
using non_interruptible_errorator = crimson::errorator<
	crimson::EmptyInterruptCondition,
	crimson::interrupt_error_carrier<
	  crimson::common::esuccess>>;

template<typename INT_COND_BUILDER, bool may_loop = true, typename OpFunc, typename OnInterrupt, typename Result = std::result_of_t<OpFunc()>>
inline seastar::future<> with_interruption(OpFunc&& func, OnInterrupt&& efunc)
{
  if constexpr (may_loop) {
    return crimson::do_until([func=std::move(func),
			     efunc=std::move(efunc)]() mutable {
      return interruption_errorator<INT_COND_BUILDER>::template futurize<
		std::result_of_t<OpFunc()>>::apply(std::move(func), std::make_tuple())
      .template handle_error<false>(std::move(efunc));
    }).safe_then([] { return seastar::now(); },
		 typename interruption_errorator<INT_COND_BUILDER>::assert_all());
  } else {
    return interruption_errorator<INT_COND_BUILDER>::template futurize<
	      std::result_of_t<OpFunc()>>::apply(std::move(func), std::make_tuple())
    .handle_error(std::move(efunc));
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
