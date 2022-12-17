#pragma once

#include <seastar/core/future.hh>
#include "crimson/common/errorator.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/pg_backend.h"

namespace crimson::osd {
class ObjectContextLoader {
public:
  using obc_accessing_list_t = boost::intrusive::list<
    ObjectContext,
    ObjectContext::obc_accessing_option_t>;

  ObjectContextLoader(
    ShardServices& _shard_services,
    PGBackend* _backend)
    : shard_services{_shard_services},
      backend{_backend}
    {}

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;

  using with_obc_func_t =
    std::function<load_obc_iertr::future<> (ObjectContextRef)>;

  template<RWState::State State>
  load_obc_iertr::future<> with_obc(hobject_t oid,
				    Ref<PG> pg,
                                    with_obc_func_t&& func);

  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc(hobject_t oid,
					  Ref<PG> pg,
                                          with_obc_func_t&& func);

  // Use this variant in the case where the head object
  // obc is already locked. Avoid nesting
  // with_head_obc() as in using with_clone_obc().
  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc_only(ObjectContextRef head,
                                               hobject_t oid,
					       Ref<PG> pg,
                                               with_obc_func_t&& func);

  template<RWState::State State>
  load_obc_iertr::future<> with_head_obc(ObjectContextRef obc,
                                         bool existed,
					 Ref<PG> pg,
                                         with_obc_func_t&& func);

  template<RWState::State State>
  load_obc_iertr::future<ObjectContextRef>
  get_or_load_obc(ObjectContextRef obc,
                  bool existed,
		  Ref<PG> pg);

  load_obc_iertr::future<ObjectContextRef>
  load_obc(ObjectContextRef obc, Ref<PG> pg);

  load_obc_iertr::future<> reload_obc(ObjectContext& obc) const;

  void notify_on_change(bool is_primary);

private:
  ShardServices &shard_services;
  PGBackend* backend;
  obc_accessing_list_t obc_set_accessing;
};
}
