// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>
#include "include/buffer_fwd.h"
#include "osd/osd_types.h"

#include "acked_peers.h"
#include "pg_backend.h"

namespace crimson::osd {
  class ShardServices;
}

class MOSDPGPull;

class ReplicatedBackend : public PGBackend
{
public:
  ReplicatedBackend(pg_t pgid, pg_shard_t whoami,
		    CollectionRef coll,
		    crimson::osd::ShardServices& shard_services,
		    crimson::osd::PG& pg);
  void got_rep_op_reply(const MOSDRepOpReply& reply) final;

  // local modifications
  seastar::future<> local_remove(
    ObjectState& os) final;
  seastar::future<> do_local_transaction(
    ceph::os::Transaction& txn) final;
  seastar::future<crimson::os::FuturizedStore::OmapIterator> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& iod) final;

  // recovery related
  seastar::future<> recover_object(
    const hobject_t& soid,
    eversion_t need) final;
  seastar::future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) final;
  seastar::future<> push_delete(
    const hobject_t& soid,
    eversion_t need) final;
  seastar::future<> handle_pull(
    MOSDPGPull& m) final;
  seastar::future<> handle_pull_response(
    const MOSDPGPush& m) final;
  seastar::future<> handle_push(
    const MOSDPGPush& m) final;
  seastar::future<> handle_push_reply(
    const MOSDPGPushReply& m) final;
  seastar::future<> handle_recovery_delete(
    const MOSDPGRecoveryDelete& m) final;

private:
  ll_read_errorator::future<ceph::bufferlist> _read(const hobject_t& hoid,
					            uint64_t off,
					            uint64_t len,
					            uint32_t flags) override;
  seastar::future<crimson::osd::acked_peers_t>
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      const osd_op_params_t& osd_op_p,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) final;
  const pg_t pgid;
  const pg_shard_t whoami;
  crimson::osd::ShardServices& shard_services;
  ceph_tid_t next_txn_id = 0;
  struct pending_on_t {
    pending_on_t(size_t pending)
      : pending{static_cast<unsigned>(pending)}
    {}
    unsigned pending;
    crimson::osd::acked_peers_t acked_peers;
    seastar::promise<> all_committed;
  };
  using pending_transactions_t = std::map<ceph_tid_t, pending_on_t>;
  pending_transactions_t pending_trans;

  // recovery related
  seastar::future<> prep_push(
    const hobject_t& soid,
    eversion_t need,
    PushOp* pop,
    const std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>& shards);
  void prepare_pull(
    PullOp& po,
    PullInfo& pi,
    const hobject_t& soid,
    eversion_t need);
  std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator> get_shards_to_push(
    const hobject_t& soid);
  seastar::future<ObjectRecoveryProgress> build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat,
    PushOp* pop);
  seastar::future<bool> _handle_pull_response(
    pg_shard_t from,
    const PushOp& pop,
    PullOp* response,
    ceph::os::Transaction* t);
  void trim_pushed_data(
    const interval_set<uint64_t> &copy_subset,
    const interval_set<uint64_t> &intervals_received,
    ceph::bufferlist data_received,
    interval_set<uint64_t> *intervals_usable,
    bufferlist *data_usable);
  seastar::future<> submit_push_data(
    const ObjectRecoveryInfo &recovery_info,
    bool first,
    bool complete,
    bool clear_omap,
    interval_set<uint64_t> &data_zeros,
    const interval_set<uint64_t> &intervals_included,
    ceph::bufferlist data_included,
    ceph::bufferlist omap_header,
    const std::map<string, bufferlist> &attrs,
    const std::map<string, bufferlist> &omap_entries,
    ceph::os::Transaction *t);
  void submit_push_complete(
    const ObjectRecoveryInfo &recovery_info,
    ObjectStore::Transaction *t);
  seastar::future<> _handle_push(
    pg_shard_t from,
    const PushOp &pop,
    PushReplyOp *response,
    ceph::os::Transaction *t);
  seastar::future<bool> _handle_push_reply(
    pg_shard_t peer,
    const PushReplyOp &op,
    PushOp *reply);
  seastar::future<> on_local_recover_persist(
    const hobject_t& soid,
    const ObjectRecoveryInfo& _recovery_info,
    bool is_delete,
    ceph::os::Transaction& t,
    epoch_t epoch_to_freeze);
  seastar::future<> local_recover_delete(
    const hobject_t& soid,
    eversion_t need,
    epoch_t epoch_frozen);
};
