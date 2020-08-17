// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_request.h"

#include "common/Formatter.h"
#include "messages/MOSDRepOp.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

RepRequest::RepRequest(OSD &osd,
		       crimson::net::ConnectionRef&& conn,
		       Ref<MOSDRepOp> &&req)
  : osd{osd},
    conn{std::move(conn)},
    req{req}
{}

void RepRequest::print(std::ostream& os) const
{
  os << "RepRequest("
     << "from=" << req->from
     << " req=" << *req
     << ")";
}

void RepRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("RepRequest");
  f->dump_stream("reqid") << req->reqid;
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("oid") << req->poid;
  f->dump_stream("from") << req->from;
  f->close_section();
}

RepRequest::ConnectionPipeline &RepRequest::cp()
{
  return get_osd_priv(conn.get()).replicated_request_conn_pipeline;
}

RepRequest::PGPipeline &RepRequest::pp(PG &pg)
{
  return pg.replicated_request_pg_pipeline;
}

seastar::future<> RepRequest::start()
{
  logger().debug("{} start", *this);
  IRef ref = this;
  return crimson::common::with_interruption<IOInterruptConditionBuilder, false>([this, ref=std::move(ref)]() mutable {
    return with_blocking_errorated_future<interruption_errorator>(
		handle.enter(cp().await_map)).safe_then([this]() {
      return with_blocking_errorated_future<interruption_errorator>(
		osd.osdmap_gate.wait_for_map_errorated(req->get_min_epoch()));
    }).safe_then([this](epoch_t epoch) {
      return with_blocking_errorated_future<interruption_errorator>(
		handle.enter(cp().get_pg));
    }).safe_then([this] {
      return with_blocking_errorated_future<interruption_errorator>(
		osd.wait_for_pg_errorated(req->get_spg()));
    }).safe_then([this, ref=std::move(ref)](Ref<PG> pg) {
      return pg->handle_rep_op(std::move(req));
    });
  }, interruption_errorator::all_same_way(
    [](const std::error_code& e) {
    if (e == crimson::common::ec<crimson::common::error::actingset_change>) {
      crimson::get_logger(ceph_subsys_osd).debug(
	  "operation restart, acting set changed");
      return seastar::now();
    } else if (e == crimson::common::ec<crimson::common::error::system_shutdown>) {
      crimson::get_logger(ceph_subsys_osd).debug(
	"operation restart, acting set changed");
      return seastar::now();
    }
  }));
}
}
