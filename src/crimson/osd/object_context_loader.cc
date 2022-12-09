// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/object_context_loader.h"
#include "crimson/osd/watch.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

using crimson::common::local_conf;

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_head_obc(ObjectContextRef obc,
                                     bool existed,
				     Ref<PG> pg,
                                     with_obc_func_t&& func)
  {
    logger().debug("{} {}", __func__, obc->get_oid());
    assert(obc->is_head());
    obc->append_to(obc_set_accessing);
    return obc->with_lock<State, IOInterruptCondition>(
      [existed=existed, obc=obc, func=std::move(func), this, pg] {
      return get_or_load_obc<State>(obc, existed, pg)
      .safe_then_interruptible(
        [func = std::move(func)](auto obc) {
        return std::move(func)(std::move(obc));
      });
    }).finally([this, obc=std::move(obc)] {
      logger().debug("with_head_obc: released {}", obc->get_oid());
      obc->remove_from(obc_set_accessing);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc(hobject_t oid,
				      Ref<PG> pg,
                                      with_obc_func_t&& func)
  {
    assert(!oid.is_head());
    return with_obc<RWState::RWREAD>(oid.get_head(), pg,
      [oid, func=std::move(func), this,
      pg](auto head) mutable
      -> load_obc_iertr::future<> {
      if (!head->obs.exists) {
        logger().error("with_clone_obc: {} head doesn't exist",
                       head->obs.oi.soid);
        return load_obc_iertr::future<>{
          crimson::ct_error::enoent::make()
        };
      }
      return this->with_clone_obc_only<State>(head,
                                              oid,
					      std::move(pg),
                                              std::move(func));
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_clone_obc_only(ObjectContextRef head,
                                           hobject_t oid,
					   Ref<PG> pg,
                                           with_obc_func_t&& func)
  {
    auto coid = resolve_oid(head->get_ro_ss(), oid);
    if (!coid) {
      logger().error("with_clone_obc_only: {} clone not found",
                     coid);
      return load_obc_iertr::future<>{
        crimson::ct_error::enoent::make()
      };
    }
    auto [clone, existed] = shard_services.get_cached_obc(*coid);
    return clone->template with_lock<State, IOInterruptCondition>(
      [existed=existed, clone=std::move(clone), pg=std::move(pg),
       func=std::move(func), head=std::move(head), this]()
      -> load_obc_iertr::future<> {
      auto loaded = get_or_load_obc<State>(clone, existed, std::move(pg));
      clone->head = std::move(head);
      return loaded.safe_then_interruptible(
        [func = std::move(func)](auto clone) {
        return std::move(func)(std::move(clone));
      });
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc(hobject_t oid,
				Ref<PG> pg,
                                with_obc_func_t&& func)
  {
    if (oid.is_head()) {
      auto [obc, existed] =
        shard_services.get_cached_obc(std::move(oid));
      return with_head_obc<State>(std::move(obc),
                                  existed,
				  std::move(pg),
                                  std::move(func));
    } else {
      return with_clone_obc<State>(oid, std::move(pg), std::move(func));
    }
  }

  ObjectContextLoader::load_obc_iertr::future<ObjectContextRef>
  ObjectContextLoader::load_obc(ObjectContextRef obc, Ref<PG> pg)
  {
    return backend->load_metadata(obc->get_oid())
    .safe_then_interruptible(
      [obc=std::move(obc), pg=std::move(pg)](auto md)
      -> load_obc_ertr::future<ObjectContextRef> {
      const hobject_t& oid = md->os.oi.soid;
      logger().debug(
        "load_obc: loaded obs {} for {}", md->os.oi, oid);
      if (oid.is_head()) {
        if (!md->ssc) {
          logger().error(
            "load_obc: oid {} missing snapsetcontext", oid);
          return crimson::ct_error::object_corrupted::make();
        }
        obc->set_head_state(std::move(md->os),
                            std::move(md->ssc));
	for (auto &[src, winfo] : obc->obs.oi.watchers) {
	  auto [it, emplaced] = obc->watchers.emplace(src, nullptr);
	  assert(emplaced);
	  logger().debug("adding watch for obj {}, client {}",
	    obc->get_oid(), src.second);
	  it->second = crimson::osd::Watch::create(
	    obc, winfo, src.second, std::move(pg));
	  it->second->disconnect();
	}
      } else {
        obc->set_clone_state(std::move(md->os));
      }
      logger().debug(
        "load_obc: returning obc {} for {}",
        obc->obs.oi, obc->obs.oi.soid);
      return load_obc_ertr::make_ready_future<ObjectContextRef>(obc);
    });
  }

  template<RWState::State State>
  ObjectContextLoader::load_obc_iertr::future<ObjectContextRef>
  ObjectContextLoader::get_or_load_obc(ObjectContextRef obc,
                                       bool existed,
				       Ref<PG> pg)
  {
    auto loaded =
      load_obc_iertr::make_ready_future<ObjectContextRef>(obc);
    if (existed) {
      logger().debug("{}: found {} in cache",
                     __func__, obc->get_oid());
    } else {
      logger().debug("{}: cache miss on {}",
                     __func__, obc->get_oid());
      loaded =
        obc->template with_promoted_lock<State, IOInterruptCondition>(
        [obc, this, pg] {
        return load_obc(obc, pg);
      });
    }
    return loaded;
  }

  ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::reload_obc(ObjectContext& obc) const
  {
    assert(obc.is_head());
    return backend->load_metadata(obc.get_oid())
    .safe_then_interruptible<false>(
      [&obc](auto md)-> load_obc_ertr::future<> {
      logger().debug(
        "{}: reloaded obs {} for {}",
        __func__,
        md->os.oi,
        obc.get_oid());
      if (!md->ssc) {
        logger().error(
          "{}: oid {} missing snapsetcontext",
          __func__,
          obc.get_oid());
        return crimson::ct_error::object_corrupted::make();
      }
      obc.set_head_state(std::move(md->os), std::move(md->ssc));
      return load_obc_ertr::now();
    });
  }

  void ObjectContextLoader::notify_on_change(bool is_primary)
  {
    for (auto& obc : obc_set_accessing) {
      obc.interrupt(::crimson::common::actingset_changed(is_primary));
    }
  }

  // explicitly instantiate the used instantiations
  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWNONE>(hobject_t,
						 Ref<PG>,
                                                 with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWREAD>(hobject_t,
						 Ref<PG>,
                                                 with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWWRITE>(hobject_t,
						  Ref<PG>,
                                                  with_obc_func_t&&);

  template ObjectContextLoader::load_obc_iertr::future<>
  ObjectContextLoader::with_obc<RWState::RWEXCL>(hobject_t,
						 Ref<PG>,
                                                 with_obc_func_t&&);
}
