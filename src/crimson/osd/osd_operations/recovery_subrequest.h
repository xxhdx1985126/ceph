// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"
#include "crimson/osd/osd.h"

namespace crimson::osd {
class PG;
class OSD;

class RecoverySubRequest final : public OperationT<RecoverySubRequest> {
  OSD& osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDFastDispatchOp> m;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::background_recovery_sub;

  RecoverySubRequest(OSD &osd, crimson::net::ConnectionRef conn, Ref<MOSDFastDispatchOp>&& m)
    : osd(osd), conn(conn), m(m) {}

  void print(std::ostream& out) const final
  {
    out << *m;
  }

  void dump_detail(Formatter *f) const final
  {
  }

  seastar::future<> start() {
    return with_blocking_future(osd.osdmap_gate.wait_for_map(m->get_min_epoch()))
    .then([this] (epoch_t epoch) {
      return with_blocking_future(osd.wait_for_pg(m->get_spg()));
    }).then([this] (Ref<PG> pgref) {
      switch (m->get_header().type) {
      case MSG_OSD_PG_PULL:
	return pgref->handle_pull(*boost::static_pointer_cast<MOSDPGPull>(m));
      case MSG_OSD_PG_PUSH:
	return pgref->handle_push(*boost::static_pointer_cast<MOSDPGPush>(m));
      case MSG_OSD_PG_PUSH_REPLY:
	return pgref->handle_push_reply(
	    *boost::static_pointer_cast<MOSDPGPushReply>(m));
      case MSG_OSD_PG_RECOVERY_DELETE:
	return pgref->handle_recovery_delete(
	    *boost::static_pointer_cast<MOSDPGRecoveryDelete>(m));
      case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
	return pgref->handle_recovery_delete_reply(
	    *boost::static_pointer_cast<MOSDPGRecoveryDeleteReply>(m));
      default:
	return seastar::make_exception_future<>(
	    std::runtime_error(fmt::format("invalid request type: {}",
					   m->get_header().type)));
      }
    });
  }
};

}
