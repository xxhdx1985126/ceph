// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <string>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <boost/container/flat_set.hpp>

#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/acked_peers.h"
#include "crimson/osd/pg.h"
#include "crimson/common/shared_lru.h"
#include "osd/osd_types.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/osdop_params.h"

struct hobject_t;
class MOSDRepOpReply;
class MOSDPGPull;

namespace ceph::os {
  class Transaction;
}

namespace crimson::osd {
  class ShardServices;
}

class PGBackend
{
protected:
  using CollectionRef = crimson::os::CollectionRef;
  using ec_profile_t = std::map<std::string, std::string>;
  // low-level read errorator
  using ll_read_errorator = crimson::os::FuturizedStore::read_errorator;

public:
  using load_metadata_ertr = crimson::errorator<
    crimson::ct_error::object_corrupted>;
  PGBackend(shard_id_t shard, CollectionRef coll, crimson::os::FuturizedStore* store,
	    crimson::osd::PG& pg);
  virtual ~PGBackend() = default;
  static std::unique_ptr<PGBackend> create(pg_t pgid,
					   const pg_shard_t pg_shard,
					   const pg_pool_t& pool,
					   crimson::os::CollectionRef coll,
					   crimson::osd::ShardServices& shard_services,
					   const ec_profile_t& ec_profile,
					   crimson::osd::PG& pg);
  using attrs_t =
    std::map<std::string, ceph::bufferptr, std::less<>>;
  using read_errorator = ll_read_errorator::extend<
    crimson::ct_error::object_corrupted>;
  read_errorator::future<ceph::bufferlist> read(
    const object_info_t& oi,
    uint64_t off,
    uint64_t len,
    size_t truncate_size,
    uint32_t truncate_seq,
    uint32_t flags);

  using stat_errorator = crimson::errorator<crimson::ct_error::enoent>;
  stat_errorator::future<> stat(
    const ObjectState& os,
    OSDOp& osd_op);

  seastar::future<> create(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  seastar::future<> remove(
    ObjectState& os,
    ceph::os::Transaction& txn);
  seastar::future<> write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  seastar::future<> writefull(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  seastar::future<crimson::osd::acked_peers_t> mutate_object(
    std::set<pg_shard_t> pg_shards,
    crimson::osd::ObjectContextRef &&obc,
    ceph::os::Transaction&& txn,
    const osd_op_params_t& osd_op_p,
    epoch_t min_epoch,
    epoch_t map_epoch,
    std::vector<pg_log_entry_t>&& log_entries);
  seastar::future<std::vector<hobject_t>, hobject_t> list_objects(
    const hobject_t& start,
    uint64_t limit) const;
  seastar::future<> setxattr(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  using get_attr_errorator = crimson::os::FuturizedStore::get_attr_errorator;
  get_attr_errorator::future<> getxattr(
    const ObjectState& os,
    OSDOp& osd_op) const;
  get_attr_errorator::future<ceph::bufferptr> getxattr(
    const hobject_t& soid,
    std::string_view key) const;
  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) const;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len);

  // OMAP
  seastar::future<> omap_get_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_get_vals(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_get_vals_by_keys(
    const ObjectState& os,
    OSDOp& osd_op) const;
  seastar::future<> omap_set_vals(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& trans);
  seastar::future<ceph::bufferlist> omap_get_header(
    crimson::os::CollectionRef& c,
    const ghobject_t& oid);
  virtual seastar::future<crimson::os::FuturizedStore::OmapIterator>
  get_omap_iterator(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  virtual void got_rep_op_reply(const MOSDRepOpReply&) {}

  // local operations
  virtual seastar::future<> local_remove(
    ObjectState& os) = 0;
  virtual seastar::future<> do_local_transaction(
    ceph::os::Transaction& txn) = 0;

  // recovery related
  virtual seastar::future<> recover_object(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual seastar::future<> recover_delete(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual seastar::future<> push_delete(
    const hobject_t& soid,
    eversion_t need) = 0;
  virtual seastar::future<> handle_pull(
    MOSDPGPull& m) = 0;
  virtual seastar::future<> handle_pull_response(
    const MOSDPGPush& m) = 0;
  virtual seastar::future<> handle_push(
    const MOSDPGPush& m) = 0;
  virtual seastar::future<> handle_push_reply(
    const MOSDPGPushReply& m) = 0;
  virtual seastar::future<> handle_recovery_delete(
    const MOSDPGRecoveryDelete& m) = 0;
protected:
  const shard_id_t shard;
  CollectionRef coll;
  crimson::os::FuturizedStore* store;
  crimson::osd::PG& pg;

  struct PullInfo {
    pg_shard_t from;
    hobject_t soid;
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    crimson::osd::ObjectContextRef head_ctx;
    crimson::osd::ObjectContextRef obc;
    object_stat_sum_t stat;
    bool is_complete() const {
      return recovery_progress.is_complete(recovery_info);
    }
  };

  struct PushInfo {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    crimson::osd::ObjectContextRef obc;
    object_stat_sum_t stat;
  };

  class WaitForObjectRecovery : public crimson::osd::BlockerT<WaitForObjectRecovery> {
    seastar::shared_promise<> readable, recovered, pulled;
    std::map<pg_shard_t, seastar::shared_promise<>> pushes;
  public:
    static constexpr const char* type_name = "WaitForObjectRecovery";

    crimson::osd::ObjectContextRef obc;
    PullInfo pi;
    std::map<pg_shard_t, PushInfo> pushing;

    seastar::future<> wait_for_readable() {
      return readable.get_shared_future();
    }
    seastar::future<> wait_for_pushes(pg_shard_t shard) {
      return pushes[shard].get_shared_future();
    }
    seastar::future<> wait_for_recovered() {
      return recovered.get_shared_future();
    }
    seastar::future<> wait_for_pull() {
      return pulled.get_shared_future();
    }
    void set_readable() {
      readable.set_value();
    }
    void set_recovered() {
      recovered.set_value();
    }
    void set_pushed(pg_shard_t shard) {
      pushes[shard].set_value();
    }
    void set_pulled() {
      pulled.set_value();
    }
    void dump_detail(Formatter* f) const {
    }
  };
  std::map<hobject_t, WaitForObjectRecovery> recovering;
  hobject_t get_temp_recovery_object(
    const hobject_t& target,
    eversion_t version);

  boost::container::flat_set<hobject_t> temp_contents;

  void add_temp_obj(const hobject_t &oid) {
    temp_contents.insert(oid);
  }
  void clear_temp_obj(const hobject_t &oid) {
    temp_contents.erase(oid);
  }

public:
  struct loaded_object_md_t {
    ObjectState os;
    std::optional<SnapSet> ss;
    using ref = std::unique_ptr<loaded_object_md_t>;
  };
  load_metadata_ertr::future<loaded_object_md_t::ref> load_metadata(
    const hobject_t &oid);
  WaitForObjectRecovery& get_recovering(const hobject_t& soid) {
    return recovering[soid];
  }
  void remove_recovering(const hobject_t& soid) {
    recovering.erase(soid);
  }
  bool is_recovering(const hobject_t& soid) {
    return recovering.count(soid) != 0;
  }
  uint64_t total_recovering() {
    return recovering.size();
  }

private:
  virtual ll_read_errorator::future<ceph::bufferlist> _read(
    const hobject_t& hoid,
    size_t offset,
    size_t length,
    uint32_t flags) = 0;

  bool maybe_create_new_object(ObjectState& os, ceph::os::Transaction& txn);
  virtual seastar::future<crimson::osd::acked_peers_t>
  _submit_transaction(std::set<pg_shard_t>&& pg_shards,
		      const hobject_t& hoid,
		      ceph::os::Transaction&& txn,
		      const osd_op_params_t& osd_op_p,
		      epoch_t min_epoch, epoch_t max_epoch,
		      std::vector<pg_log_entry_t>&& log_entries) = 0;
  friend class crimson::osd::PG;
};
