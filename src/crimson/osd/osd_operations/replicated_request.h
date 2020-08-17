// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/exception.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/io_interrupt_condition_builder.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"

class MOSDRepOp;

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class OSD;
class PG;

class RepRequest final : public OperationT<RepRequest> {
public:
  class ConnectionPipeline {
    OrderedPipelinePhase await_map = {
      "RepRequest::ConnectionPipeline::await_map"
    };
    OrderedPipelinePhase get_pg = {
      "RepRequest::ConnectionPipeline::get_pg"
    };
    friend RepRequest;
  };
  class PGPipeline {
    OrderedPipelinePhase await_map = {
      "RepRequest::PGPipeline::await_map"
    };
    OrderedPipelinePhase process = {
      "RepRequest::PGPipeline::process"
    };
    friend RepRequest;
  };
  static constexpr OperationTypeCode type = OperationTypeCode::replicated_request;
  RepRequest(OSD&, crimson::net::ConnectionRef&&, Ref<MOSDRepOp>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();

private:
  using interruption_errorator =
    crimson::common::interruption_errorator<
      IOInterruptConditionBuilder>;
  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDRepOp> req;
  OrderedPipelinePhase::Handle handle;
};

}
