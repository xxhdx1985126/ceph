// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ClientRequest::ClientRequest(
  OSD &osd, crimson::net::ConnectionRef conn, Ref<MOSDOp> &&m)
  : osd(osd), conn(conn), m(m)
{}

void ClientRequest::print(std::ostream &lhs) const
{
  lhs << *m;
}

void ClientRequest::dump_detail(Formatter *f) const
{
}

ClientRequest::ConnectionPipeline &ClientRequest::cp()
{
  return get_osd_priv(conn.get()).client_request_conn_pipeline;
}

ClientRequest::PGPipeline &ClientRequest::pp(PG &pg)
{
  return pg.client_request_pg_pipeline;
}

bool ClientRequest::is_pg_op() const
{
  return std::any_of(
    begin(m->ops), end(m->ops),
    [](auto& op) { return ceph_osd_op_type_pg(op.op.op); });
}

seastar::future<> ClientRequest::start()
{
  logger().debug("{}: start", *this);

  IRef opref = this;
  return crimson::common::with_interruption<IOInterruptConditionBuilder>(
    [this, opref=std::move(opref)]() mutable {
    return with_blocking_errorated_future<interruption_errorator>(
	      handle.enter(cp().await_map))
    .safe_then([this]() {
      return with_blocking_errorated_future<interruption_errorator>(
		osd.osdmap_gate.wait_for_map_errorated(m->get_min_epoch()));
    }).safe_then([this](epoch_t epoch) {
      return with_blocking_errorated_future<interruption_errorator>(
		handle.enter(cp().get_pg));
    }).safe_then([this] {
      return with_blocking_errorated_future<interruption_errorator>(
		  osd.wait_for_pg_errorated(m->get_spg()));
    }).safe_then([this, opref](Ref<PG> pgref) {
      PG &pg = *pgref;
      if (__builtin_expect(m->get_map_epoch()
			    < pg.get_info().history.same_primary_since,
			   false)) {
	return interruption_errorator::future<>(
		osd.send_incremental_map(conn.get(), m->get_map_epoch()));
      }
      return with_blocking_errorated_future<interruption_errorator>(
	handle.enter(pp(pg).await_map)
      ).safe_then([this, &pg]() mutable {
	return with_blocking_errorated_future<interruption_errorator>(
		pg.osdmap_gate.wait_for_map_errorated(m->get_map_epoch()));
      }).safe_then([this, &pg](auto map) mutable {
	return with_blocking_errorated_future<interruption_errorator>(
		handle.enter(pp(pg).wait_for_active));
      }).safe_then([this, &pg]() mutable {
	return with_blocking_errorated_future<interruption_errorator>(
		pg.wait_for_active_blocker.wait_errorated());
      }).safe_then([this, pgref=std::move(pgref)]() mutable
	-> interruption_errorator::future<> {
	if (m->finish_decode()) {
	  m->clear_payload();
	}
	if (is_pg_op()) {
	  return process_pg_op(pgref);
	} else {
	  return process_op(pgref);
	}
      });
    }).safe_then([] {
      return true;
    });
  }, interruption_errorator::all_same_way(
    [](const std::error_code& e) {
    if (e == crimson::common::ec<crimson::common::error::actingset_change>) {
      crimson::get_logger(ceph_subsys_osd).debug(
	  "operation restart, acting set changed");
      return interruption_errorator::make_ready_future<bool>(false);
    } else if (e == crimson::common::ec<crimson::common::error::system_shutdown>) {
      crimson::get_logger(ceph_subsys_osd).debug(
	"operation restart, acting set changed");
      return interruption_errorator::make_ready_future<bool>(true);
    }
  }));
}

seastar::future<> ClientRequest::process_pg_op(
  Ref<PG> &pg)
{
  return pg->do_pg_ops(m)
    .then([this, pg=std::move(pg)](Ref<MOSDOpReply> reply) {
      return conn->send(reply);
    });
}

ClientRequest::interruption_errorator::future<> ClientRequest::process_op(
  Ref<PG> &pgref)
{
  PG& pg = *pgref;
  return with_blocking_errorated_future<interruption_errorator>(
    handle.enter(pp(pg).recover_missing)
  ).safe_then([this, &pg, pgref]()
    -> interruption_errorator::future<> {
    eversion_t ver;
    const hobject_t& soid = m->get_hobj();
    if (pg.is_unreadable_object(soid, &ver)) {
      auto [op, fut] = osd.get_shard_services().start_operation<UrgentRecovery>(
			  soid, ver, pgref, osd.get_shard_services(), m->get_min_epoch());
      return std::move(fut);
    }
    return interruption_errorator::now();
  }).safe_then([this, &pg] {
    return with_blocking_errorated_future<interruption_errorator>(
	handle.enter(pp(pg).get_obc));
  }).safe_then([this, &pg]() {
    op_info.set_from_op(&*m, *pg.get_osdmap());
    return pg.with_locked_obc(
      m,
      op_info,
      this,
      [this, &pg](auto obc) {
	return with_blocking_errorated_future<interruption_errorator>(
	    handle.enter(pp(pg).process)
	).safe_then([this, &pg, obc]() {
	  return pg.do_osd_ops(m, obc, op_info);
	}).safe_then([this](Ref<MOSDOpReply> reply) {
	  return conn->send(reply);
	});
      });
  }).safe_then([pgref=std::move(pgref)] {
    return seastar::now();
  }, PG::load_obc_ertr::all_same_way([](auto &code) {
    logger().error("ClientRequest saw error code {}", code);
    return interruption_errorator::now();
  }),interruption_errorator::pass_further{});
}

}
