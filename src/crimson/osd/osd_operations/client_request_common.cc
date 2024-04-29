// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/background_recovery.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

SET_SUBSYS(osd);

namespace crimson::osd {

InterruptibleOperation::template interruptible_future<>
CommonClientRequest::recover_missings(
  Ref<PG> &pg,
  const hobject_t& soid,
  std::set<snapid_t> &&snaps,
  const osd_reqid_t& reqid)
{
  LOG_PREFIX(CommonClientRequest::recover_missings);
  if (!pg->is_primary()) {
    DEBUGDPP(
      "Skipping recover_missings on non primary pg for soid {}", *pg, soid);
    return seastar::now();
  }
  return do_recover_missing(
    pg, soid.get_head(), reqid
  ).then_interruptible([snaps=std::move(snaps), pg, soid, reqid]() mutable {
    return pg->obc_loader.with_obc<RWState::RWREAD>(
      soid.get_head(),
      [snaps=std::move(snaps), pg, soid, reqid](auto head, auto) mutable {
      return seastar::do_with(
	std::move(snaps),
	[pg, soid, head, reqid](auto &snaps) mutable {
	return InterruptibleOperation::interruptor::do_for_each(
	  snaps,
	  [pg, soid, head, reqid](auto &snap) mutable ->
	  InterruptibleOperation::template interruptible_future<> {
	  auto coid = head->obs.oi.soid;
	  coid.snap = snap;
	  auto oid = resolve_oid(head->get_head_ss(), coid);
	  /* Rollback targets may legitimately not exist if, for instance,
	   * the object is an rbd block which happened to be sparse and
	   * therefore non-existent at the time of the specified snapshot.
	   * In such a case, rollback will simply delete the object.  Here,
	   * we skip the oid as there is no corresponding clone to recover.
	   * See https://tracker.ceph.com/issues/63821 */
	  if (oid) {
	    return do_recover_missing(pg, *oid, reqid);
	  } else {
	    return seastar::now();
	  }
	});
      });
    });
  }).handle_error_interruptible(
    crimson::ct_error::assert_all("unexpected error")
  );
}

typename InterruptibleOperation::template interruptible_future<>
CommonClientRequest::do_recover_missing(
  Ref<PG>& pg,
  const hobject_t& soid,
  const osd_reqid_t& reqid)
{
  eversion_t ver;
  assert(pg->is_primary());
  logger().debug("{} reqid {} check for recovery, {}",
                 __func__, reqid, soid);
  auto &peering_state = pg->get_peering_state();
  auto &missing_loc = peering_state.get_missing_loc();
  bool needs_recovery = missing_loc.needs_recovery(soid, &ver);
  if (!pg->is_unreadable_object(soid) &&
      !pg->is_degraded_or_backfilling_object(soid)) {
    logger().debug("{} reqid {} nothing to recover {}",
                   __func__, reqid, soid);
    return seastar::now();
  }
  ceph_assert(needs_recovery);

  logger().debug("{} reqid {} need to wait for recovery, {} version {}",
                 __func__, reqid, soid, ver);
  if (pg->get_recovery_backend()->is_recovering(soid)) {
    logger().debug("{} reqid {} object {} version {}, already recovering",
                   __func__, reqid, soid, ver);
    return pg->get_recovery_backend()->get_recovering(soid).wait_for_recovered();
  } else {
    logger().debug("{} reqid {} object {} version {}, starting recovery",
                   __func__, reqid, soid, ver);
    auto [op, fut] =
      pg->get_shard_services().start_operation<UrgentRecovery>(
        soid, ver, pg, pg->get_shard_services(), pg->get_osdmap_epoch());
    return std::move(fut);
  }
}

bool CommonClientRequest::should_abort_request(
  const Operation& op,
  std::exception_ptr eptr)
{
  if (*eptr.__cxa_exception_type() ==
      typeid(::crimson::common::actingset_changed)) {
    try {
      std::rethrow_exception(eptr);
    } catch(::crimson::common::actingset_changed& e) {
      if (e.is_primary()) {
        logger().debug("{} {} operation restart, acting set changed", __func__, op);
        return false;
      } else {
        logger().debug("{} {} operation abort, up primary changed", __func__, op);
        return true;
      }
    }
  } else {
    assert(*eptr.__cxa_exception_type() ==
      typeid(crimson::common::system_shutdown_exception));
    crimson::get_logger(ceph_subsys_osd).debug(
        "{} {} operation skipped, system shutdown", __func__, op);
    return true;
  }
}

} // namespace crimson::osd
