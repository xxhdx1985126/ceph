// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;

class ClientRequest final : public OperationT<ClientRequest> {
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDOp> m;
  OpInfo op_info;
  PipelineHandle handle;

public:
  class ConnectionPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::ConnectionPipeline::await_map"
    };
    OrderedExclusivePhase get_pg = {
      "ClientRequest::ConnectionPipeline::get_pg"
    };
    friend class ClientRequest;
  };
  class PGPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::PGPipeline::await_map"
    };
    OrderedExclusivePhase wait_for_active = {
      "ClientRequest::PGPipeline::wait_for_active"
    };
    OrderedExclusivePhase recover_missing = {
      "ClientRequest::PGPipeline::recover_missing"
    };
    OrderedExclusivePhase get_obc = {
      "ClientRequest::PGPipeline::get_obc"
    };
    OrderedExclusivePhase process = {
      "ClientRequest::PGPipeline::process"
    };
    OrderedConcurrentPhase wait_repop = {
      "ClientRequest::PGPipeline::wait_repop"
    };
    friend class ClientRequest;
  };

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, crimson::net::ConnectionRef, Ref<MOSDOp> &&m);
  ~ClientRequest();

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

public:
  seastar::future<> start();

private:
  seastar::future<> process_pg_op(Ref<PG>& pg);
  seastar::future<> process_op(Ref<PG>& pg);
  seastar::future<> do_recover_missing(Ref<PG>& pgref);
  seastar::future<> do_process(
      Ref<PG>& pg,
      crimson::osd::ObjectContextRef obc);

  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  OpSequencer& sequencer;
  const uint64_t prev_op_id;

private:
  bool is_misdirected(const PG& pg) const;
};

}
