// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "replicated_backend.h"

#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/pg.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

ReplicatedBackend::ReplicatedBackend(pg_t pgid,
                                     pg_shard_t whoami,
                                     ReplicatedBackend::CollectionRef coll,
                                     crimson::osd::ShardServices& shard_services,
				     crimson::osd::PG& pg)
  : PGBackend{whoami.shard, coll, &shard_services.get_store(), pg},
    pgid{pgid},
    whoami{whoami},
    shard_services{shard_services}
{}

ReplicatedBackend::ll_read_errorator::future<ceph::bufferlist>
ReplicatedBackend::_read(const hobject_t& hoid,
                         const uint64_t off,
                         const uint64_t len,
                         const uint32_t flags)
{
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}

seastar::future<crimson::osd::acked_peers_t>
ReplicatedBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                                       const hobject_t& hoid,
                                       ceph::os::Transaction&& txn,
                                       const osd_op_params_t& osd_op_p,
                                       epoch_t min_epoch, epoch_t map_epoch,
				       std::vector<pg_log_entry_t>&& log_entries)
{
  const ceph_tid_t tid = next_txn_id++;
  auto req_id = osd_op_p.req->get_reqid();
  auto pending_txn =
    pending_trans.emplace(tid, pending_on_t{pg_shards.size()}).first;
  bufferlist encoded_txn;
  encode(txn, encoded_txn);

  return seastar::parallel_for_each(std::move(pg_shards),
    [=, encoded_txn=std::move(encoded_txn), txn=std::move(txn)]
    (auto pg_shard) mutable {
      if (pg_shard == whoami) {
        return shard_services.get_store().do_transaction(coll,std::move(txn));
      } else {
        auto m = make_message<MOSDRepOp>(req_id, whoami,
                                         spg_t{pgid, pg_shard.shard}, hoid,
                                         CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
                                         map_epoch, min_epoch,
                                         tid, osd_op_p.at_version);
        m->set_data(encoded_txn);
        pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
	encode(log_entries, m->logbl);
	m->pg_trim_to = osd_op_p.pg_trim_to;
	m->min_last_complete_ondisk = osd_op_p.min_last_complete_ondisk;
	m->set_rollback_to(osd_op_p.at_version);
        // TODO: set more stuff. e.g., pg_states
        return shard_services.send_to_osd(pg_shard.osd, std::move(m), map_epoch);
      }
    }).then([&peers=pending_txn->second] {
      if (--peers.pending == 0) {
        peers.all_committed.set_value();
      }
      return peers.all_committed.get_future();
    }).then([tid, pending_txn, this] {
      pending_txn->second.all_committed = {};
      auto acked_peers = std::move(pending_txn->second.acked_peers);
      pending_trans.erase(pending_txn);
      return seastar::make_ready_future<crimson::osd::acked_peers_t>(std::move(acked_peers));
    });
}

void ReplicatedBackend::got_rep_op_reply(const MOSDRepOpReply& reply)
{
  auto found = pending_trans.find(reply.get_tid());
  if (found == pending_trans.end()) {
    logger().warn("{}: no matched pending rep op: {}", __func__, reply);
    return;
  }
  auto& peers = found->second;
  for (auto& peer : peers.acked_peers) {
    if (peer.shard == reply.from) {
      peer.last_complete_ondisk = reply.get_last_complete_ondisk();
      if (--peers.pending == 0) {
        peers.all_committed.set_value();    
      }
      return;
    }
  }
}

seastar::future<> ReplicatedBackend::local_remove(ObjectState& os)
{
  ceph::os::Transaction txn;
  return remove(os, txn).then([=, txn=std::move(txn)] () mutable {
	return do_local_transaction(txn);
      });
}

seastar::future<> ReplicatedBackend::do_local_transaction(ceph::os::Transaction& txn)
{
  return shard_services.get_store().do_transaction(coll, std::move(txn));
}

seastar::future<crimson::os::FuturizedStore::OmapIterator>
ReplicatedBackend::get_omap_iterator(
  CollectionRef ch,
  const ghobject_t& oid)
{
  return shard_services.get_store().get_omap_iterator(ch, oid);
}

seastar::future<> ReplicatedBackend::recover_object(
    const hobject_t& soid,
    eversion_t need
    ) {
  pg_missing_tracker_t local_missing = pg.get_local_missing();

  auto to_pull_or_not = [=, &local_missing] {
    if (local_missing.is_missing(soid)) {
      PullOp po;
      auto& wfor = recovering[soid];
      auto& pi = wfor.pi;

      prepare_pull(po, pi, soid, need);
      auto msg = make_message<MOSDPGPull>();
      msg->from = pg.get_pg_whoami();
      msg->set_priority(pg.get_recovery_op_priority());
      msg->pgid = pg.get_pgid();
      msg->map_epoch = pg.get_osdmap_epoch();
      msg->min_epoch = pg.get_last_peering_reset();
      std::vector<PullOp> pulls;
      pulls.push_back(po);
      msg->set_pulls(&pulls);
      shard_services.send_to_osd(pi.from.osd,
				 std::move(msg),
				 pg.get_osdmap_epoch());
      return wfor.wait_for_pull();
    } else {
      return seastar::make_ready_future<>();
    }
  };
  
  auto shards = get_shards_to_push(soid);
  std::unique_ptr<PushOp> pop =
    std::make_unique<PushOp>();
  return to_pull_or_not().then([=, pop = pop.get(),
    shards = std::move(shards)] () mutable {
    return prep_push(soid, need, pop, shards);
  }).handle_exception([=] (auto e) {
    auto& wfor = recovering[soid];
    if (wfor.obc)
      wfor.obc->drop_recovery_read();
    recovering.erase(soid);
    return seastar::make_exception_future<>(e);
  }).then([=, pop = std::move(pop), shards = std::move(shards)] {
    return seastar::parallel_for_each(shards, [=, &pop] (auto j) {
      auto msg = make_message<MOSDPGPush>();
      msg->from = pg.get_pg_whoami();
      msg->pgid = pg.get_pgid();
      msg->map_epoch = pg.get_osdmap_epoch();
      msg->min_epoch = pg.get_last_peering_reset();
      msg->set_priority(pg.get_recovery_op_priority());
      msg->pushes.push_back(*pop);
      shard_services.send_to_osd(j->first.osd, std::move(msg), pg.get_osdmap_epoch());
      return recovering[soid].wait_for_pushes(j->first);
    });
  }).then([=, &pop, &shards, &local_missing] {
    bool error = recovering[soid].pi.recovery_progress.error;
    if (!error) {
      auto& stat = recovering[soid].pushing.begin()->second.stat;
      pg.on_global_recover(soid, stat, false);
      return seastar::make_ready_future<>();
    } else {
      auto& wfor = recovering[soid];
      if (wfor.obc)
	wfor.obc->drop_recovery_read();
      recovering.erase(soid);
      return seastar::make_exception_future<>(
	  std::runtime_error(fmt::format("Errors during pushing for {}", soid)));
    }
  });
}

seastar::future<> ReplicatedBackend::push_delete(
  const hobject_t& soid,
  eversion_t need)
{
  recovering[soid];
  epoch_t min_epoch = pg.get_last_peering_reset();

  assert(pg.get_acting_recovery_backfill.size() > 0);
  return seastar::parallel_for_each(pg.get_acting_recovery_backfill(),
    [=] (pg_shard_t shard) {
    if (shard == pg.get_pg_whoami())
      return seastar::make_ready_future<>();
    auto iter = pg.get_shard_missing().find(shard);
    if (iter == pg.get_shard_missing().end())
      return seastar::make_ready_future<>();
    if (iter->second.is_missing(soid)) {
      logger().debug("{} will remove {} from {}", __func__, soid, shard);
      pg.begin_peer_recover(shard, soid);
      spg_t target_pg = spg_t(pg.get_info().pgid.pgid, shard.shard);
      auto msg = make_message<MOSDPGRecoveryDelete>(
	  pg.get_pg_whoami(), target_pg, pg.get_osdmap_epoch(), min_epoch);

      msg->set_priority(pg.get_recovery_op_priority());
      msg->objects.push_back(std::make_pair(soid, need));
      shard_services.send_to_osd(shard.osd, std::move(msg), pg.get_osdmap_epoch());
      return recovering[soid].wait_for_pushes(shard);
    }
    return seastar::make_ready_future<>();
  });
}

seastar::future<> ReplicatedBackend::handle_recovery_delete(
  const MOSDPGRecoveryDelete& m)
{
  auto& p = m.objects.front(); //TODO: only one delete per message for now.
  return local_recover_delete(p.first, p.second, pg.get_osdmap_epoch()).then(
    [=, &m, &p] {
    auto reply = make_message<MOSDPGRecoveryDeleteReply>();
    reply->from = pg.get_pg_whoami();
    reply->set_priority(m.get_priority());
    reply->pgid = spg_t(pg.get_info().pgid.pgid, m.from.shard);
    reply->map_epoch = m.map_epoch;
    reply->min_epoch = m.min_epoch;
    reply->objects = m.objects;
    return shard_services.send_to_osd(m.from.osd, std::move(reply), pg.get_osdmap_epoch());
  });
}

seastar::future<> ReplicatedBackend::on_local_recover_persist(
  const hobject_t& soid,
  const ObjectRecoveryInfo& _recovery_info,
  bool is_delete,
  ceph::os::Transaction& t,
  epoch_t epoch_frozen)
{
  pg.on_local_recover(soid, _recovery_info, true, t);
  return do_local_transaction(t).then(
    [=, &soid, &_recovery_info,
    last_complete = pg.get_info().last_complete] {
    pg._committed_pushed_object(epoch_frozen, last_complete);
    return seastar::make_ready_future<>();
  });
}

seastar::future<> ReplicatedBackend::local_recover_delete(
  const hobject_t& soid,
  eversion_t need,
  epoch_t epoch_to_freeze)
{
  return load_metadata(soid).safe_then([=]
    (auto lomt) {
    if (lomt->os.exists)
      return local_remove(lomt->os);
    else
      return seastar::make_ready_future<>();
  }).safe_then([=] {
    ceph::os::Transaction txn;
    auto& wfor = recovering[soid];
    auto& pi = wfor.pi;
    pi.recovery_info.soid = soid;
    pi.recovery_info.version = need;
    return on_local_recover_persist(soid, pi.recovery_info,
	                            true, txn, epoch_to_freeze);
  }, PGBackend::load_metadata_ertr::all_same_way([=] (auto e) {
      ceph::os::Transaction txn;
      auto& wfor = recovering[soid];
      auto& pi = wfor.pi;
      pi.recovery_info.soid = soid;
      pi.recovery_info.version = need;
      return on_local_recover_persist(soid, pi.recovery_info,
				      true, txn, epoch_to_freeze);
    })
  );
}

seastar::future<> ReplicatedBackend::recover_delete(
  const hobject_t &soid, eversion_t need)
{
  epoch_t cur_epoch = pg.get_osdmap_epoch();
  std::unique_ptr<object_stat_sum_t> stat_diff = std::make_unique<object_stat_sum_t>();
  return local_recover_delete(soid, need, cur_epoch).then(
    [=, stat_diff = stat_diff.get()] {
    if (!pg.has_reset_since(cur_epoch)) {
      bool object_missing = false;
      for (const auto& shard : pg.get_acting_recovery_backfill()) {
	if (shard == pg.get_pg_whoami())
	  continue;
	if (pg.get_shard_missing(shard)->is_missing(soid)) {
	  logger().debug("{}: soid {} needs to deleted from replca {}",
	      __func__,
	      soid,
	      shard);
	  object_missing = true;
	  break;
	}
      }
      
      if (!object_missing) {
	stat_diff->num_objects_recovered = 1;
	return seastar::make_ready_future<>();
      } else {
	return push_delete(soid, need);
      }
    }
    return seastar::make_ready_future<>();
  }).then([=, stat_diff = std::move(stat_diff)] {
    pg.on_global_recover(soid, std::move(*stat_diff), true);
    return seastar::make_ready_future<>();
  });
}

seastar::future<> ReplicatedBackend::prep_push(
  const hobject_t& soid,
  eversion_t need,
  PushOp* pop,
  const std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>& shards
  ) {
  std::map<pg_shard_t, interval_set<uint64_t>> data_subsets;
  return seastar::parallel_for_each(shards, [=] (auto j) mutable {
    auto& wfor = recovering[soid];
    auto& obc = wfor.obc;
    auto& data_subset = data_subsets[j->first];

    data_subset.insert(0, obc->obs.oi.size);
    const auto& missing = pg.get_shard_missing().find(j->first)->second;
    if (HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS)) {
      const auto it = missing.get_items().find(soid);
      assert(it != missing.get_items().end());
      data_subset.intersection_of(it->second.clean_regions.get_dirty_regions());
      logger().debug("calc_head_subsets {} data_subset {}", soid, data_subset);
    }

    auto& pi = wfor.pushing[j->first];
    pg.begin_peer_recover(j->first, soid);
    const auto pmissing_iter = pg.get_shard_missing().find(j->first);
    const auto missing_iter = pmissing_iter->second.get_items().find(soid);
    assert(missing_iter != pmissing_iter->second.get_items().end());
    
    pi.obc = obc;
    pi.recovery_info.size = obc->obs.oi.size;
    pi.recovery_info.copy_subset = data_subset;
    pi.recovery_info.soid = soid;
    pi.recovery_info.oi = obc->obs.oi;
    pi.recovery_info.version = obc->obs.oi.version;
    pi.recovery_info.object_exist =
      missing_iter->second.clean_regions.object_is_exist();
    pi.recovery_progress.omap_complete =
      !missing_iter->second.clean_regions.omap_is_dirty() &&
      HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS);
    
    return build_push_op(pi.recovery_info, pi.recovery_progress, &pi.stat, pop)
    .then([=] (auto new_progress) {
      auto& wfor = recovering[soid];
      auto& pi = wfor.pushing[j->first];
      pi.recovery_progress = new_progress;
      return seastar::make_ready_future<>();
    });
  });
}

void ReplicatedBackend::prepare_pull(PullOp& po, PullInfo& pi,
  const hobject_t& soid,
  eversion_t need) {
  pg_missing_tracker_t local_missing = pg.get_local_missing();
  const auto missing_iter = local_missing.get_items().find(soid);
  auto m = pg.get_missing_loc_shards();
  pg_shard_t fromshard = *(m[soid].begin());

  //TODO: skipped snap objects case for now
  po.recovery_info.copy_subset.insert(0, (uint64_t) -1);
  if (HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS))
    po.recovery_info.copy_subset.intersection_of(
	missing_iter->second.clean_regions.get_dirty_regions());
  po.recovery_info.size = ((uint64_t) -1);
  po.recovery_info.object_exist =
    missing_iter->second.clean_regions.object_is_exist();
  po.soid = soid;
  po.recovery_progress.data_complete = false;
  po.recovery_progress.omap_complete =
    !missing_iter->second.clean_regions.omap_is_dirty() &&
    HAVE_FEATURE(pg.min_peer_features(), SERVER_OCTOPUS);
  po.recovery_progress.data_recovered_to = 0;
  po.recovery_progress.first = true;

  pi.from = fromshard;
  pi.soid = soid;
  pi.recovery_info = po.recovery_info;
  pi.recovery_progress = po.recovery_progress;
}

seastar::future<ObjectRecoveryProgress> ReplicatedBackend::build_push_op(
    const ObjectRecoveryInfo& recovery_info,
    const ObjectRecoveryProgress& progress,
    object_stat_sum_t* stat,
    PushOp* pop
  ) {
  std::unique_ptr<ObjectRecoveryProgress> new_progress =
    std::make_unique<ObjectRecoveryProgress>(progress);
  std::unique_ptr<object_info_t> oi =
    std::make_unique<object_info_t>();
  auto retrieve_attrs_on_first =
    [=, &recovery_info, &progress, new_progress = new_progress.get(),
    oi = oi.get()] {
    if (progress.first) {
      return omap_get_header(coll, ghobject_t(recovery_info.soid))
      .then([=, &recovery_info, &progress] (auto bl) {
	pop->omap_header.claim_append(bl);
	return store->get_attrs(coll, ghobject_t(recovery_info.soid));
      }).safe_then([=, &recovery_info, &progress] (auto attrs) mutable {
	//pop->attrset = attrs;
	for (auto p : attrs) {
	  pop->attrset[p.first].push_back(p.second);
	}
	oi->decode(pop->attrset[OI_ATTR]);
	new_progress->first = false;
	return seastar::make_ready_future<>();
      }, crimson::os::FuturizedStore::get_attrs_ertr::all_same_way(
	  [] (const std::error_code& e) {
	  return seastar::make_exception_future<>(e);
	})
      );
    }
    return seastar::make_ready_future<>();
  };

  std::unique_ptr<int> available =
    std::make_unique<int>(crimson::common::local_conf()->osd_recovery_max_chunk);
  return retrieve_attrs_on_first().then([=, &recovery_info, &progress] {
    return get_omap_iterator(coll, ghobject_t(recovery_info.soid));
  }).then([=, &recovery_info, &progress,
    available = available.get(), new_progress = new_progress.get()] (auto iter) {
    std::unique_ptr<crimson::os::FuturizedStore::OmapIterator> it =
      std::make_unique<crimson::os::FuturizedStore::OmapIterator>(iter);
    if (!progress.omap_complete) {
      auto f = [=, &recovery_info, &progress, iter = it.get()] (int ret) {
	if (!iter->valid()) {
	  new_progress->omap_complete = true;
	  return seastar::stop_iteration::yes;
	}
	
	if (!pop->omap_entries.empty()
	    && ((crimson::common::local_conf()->osd_recovery_max_omap_entries_per_chunk > 0
		&& pop->omap_entries.size()
		>= crimson::common::local_conf()->osd_recovery_max_omap_entries_per_chunk)
	      || *available <= iter->key().size() + iter->value().length())) {
	  new_progress->omap_recovered_to = iter->key();
	  return seastar::stop_iteration::yes;
	}
	
	pop->omap_entries.insert(make_pair(iter->key(), iter->value()));
	if ((iter->key().size() + iter->value().length()) <= *available)
	  *available -= (iter->key().size() + iter->value().length());
	else
	  *available = 0;
	return seastar::stop_iteration::no;
      };

      bool first = true;
      return seastar::repeat(
	[=, &recovery_info, &progress, it = std::move(it)] () mutable {
	if (first) {
	  first = false;
	  return it->lower_bound(progress.omap_recovered_to).then(f);
	} else {
	  return it->next().then(f);
	}
      });
    }
    return seastar::make_ready_future<>();
  }).then([=, &recovery_info, &progress, available = std::move(available),
    new_progress = new_progress.get()] {
    if (*available > 0) {
      std::unique_ptr<interval_set<uint64_t>> copy_subset =
	std::make_unique<interval_set<uint64_t>>(recovery_info.copy_subset);
      return fiemap(coll, ghobject_t(recovery_info.soid),
		    0, copy_subset->range_end()).then(
	[copy_subset = copy_subset.get()] (auto m) {
	interval_set<uint64_t> fiemap_included(std::move(m));
	copy_subset->intersection_of(fiemap_included);
	return seastar::make_ready_future<>();
      }).then([=, &recovery_info, &progress,
	copy_subset = copy_subset.get(), &available] {
	pop->data_included.span_of(*copy_subset,
	    progress.data_recovered_to, *available);
	if (pop->data_included.empty()) // zero filled section, skip to end!
	  new_progress->data_recovered_to = recovery_info.copy_subset.range_end();
	else
	  new_progress->data_recovered_to = pop->data_included.range_end();
	return seastar::make_ready_future<>();
      }).handle_exception([copy_subset = std::move(copy_subset)] (auto e) {
	copy_subset->clear();
	return seastar::make_ready_future<>();
      });
    } else {
      pop->data_included.clear();
      return seastar::make_ready_future<>();
    }
  }).then([=, &recovery_info, &progress, oi = oi.get()] {
    //TODO: there's no readv in cyan_store yet, use read temporarily.
    return read(recovery_info.oi, 0, pop->data_included.range_end(),
		0, 0, 0);
  }).safe_then([=, &recovery_info, &progress,
    new_progress = std::move(new_progress), oi = std::move(oi)]
    (auto bl) {
    pop->data.claim_append(bl);
    if (new_progress->is_complete(recovery_info)) {
      new_progress->data_complete = true;
      if (stat)
	stat->num_objects_recovered++;
    } else if (progress.first && progress.omap_complete) {
    // If omap is not changed, we need recovery omap
    // when recovery cannot be completed once
      new_progress->omap_complete = false;
    }

    if (stat) {
      stat->num_keys_recovered += pop->omap_entries.size();
      stat->num_bytes_recovered += pop->data.length();
    }
    pop->version = recovery_info.oi.version;
    pop->soid = recovery_info.soid;
    pop->recovery_info = recovery_info;
    pop->after_progress = *new_progress;
    pop->before_progress = progress;
    return seastar::make_ready_future<ObjectRecoveryProgress>
	      (std::move(*new_progress));
  }, PGBackend::read_errorator::all_same_way([] (const std::error_code& e) {
      return seastar::make_exception_future<ObjectRecoveryProgress>(e);
    })
  );
}

std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator>
ReplicatedBackend::get_shards_to_push(const hobject_t& soid)
{
  std::list<std::map<pg_shard_t, pg_missing_t>::const_iterator> shards;
  assert(pg.get_acting_recovery_backfill().size() > 0);
  for (set<pg_shard_t>::iterator i =
      pg.get_acting_recovery_backfill().begin();
      i != pg.get_acting_recovery_backfill().end();
      ++i) {
    if (*i == pg.get_pg_whoami())
      continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      pg.get_shard_missing().find(peer);
    assert(j != pg.get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      shards.push_back(j);
    }
  }
  return shards;
}

seastar::future<> ReplicatedBackend::handle_pull(MOSDPGPull& m)
{
  vector<PullOp> pulls;
  m.take_pulls(&pulls);
  return seastar::parallel_for_each(pulls, [=, &m, from = m.from] (auto pull_op) {
    const hobject_t& soid = pull_op.soid;
    return seastar::do_with(PushOp(), [=, &soid, &pull_op] (auto& pop) {
      return stat(coll, ghobject_t(soid)).then(
	[=, &pull_op, &pop] (auto st) {
	ObjectRecoveryInfo &recovery_info = pull_op.recovery_info;
	ObjectRecoveryProgress &progress = pull_op.recovery_progress;
	if (progress.first && recovery_info.size == ((uint64_t) -1)) {
	  // Adjust size and copy_subset
	  recovery_info.size = st.st_size;
	  if (st.st_size) {
	    interval_set<uint64_t> object_range;
	    object_range.insert(0, st.st_size);
	    recovery_info.copy_subset.intersection_of(object_range);
	  } else {
	    recovery_info.copy_subset.clear();
	  }
	  assert(recovery_info.clone_subset.empty());
	}
	return build_push_op(recovery_info, progress, 0, &pop);
      }).handle_exception([soid, &pop] (auto e) {
	pop.recovery_info.version = eversion_t();
	pop.version = eversion_t();
	pop.soid = soid;
	return seastar::make_ready_future<ObjectRecoveryProgress>();
      }).then([=, &pop, &pull_op] (auto new_progress) {
	auto msg = make_message<MOSDPGPush>();
	msg->from = pg.get_pg_whoami();
	msg->pgid = pg.get_pgid();
	msg->map_epoch = pg.get_osdmap_epoch();
	msg->min_epoch = pg.get_last_peering_reset();
	msg->set_priority(pg.get_recovery_op_priority());
	msg->pushes.push_back(pop);
	return shard_services.send_to_osd(from.osd, std::move(msg), pg.get_osdmap_epoch());
      });
    });
  });
}

seastar::future<bool> ReplicatedBackend::_handle_pull_response(
  pg_shard_t from,
  const PushOp& pop,
  PullOp* response,
  ceph::os::Transaction* t)
{
  bufferlist data;
  data = pop.data;
  logger().debug("handle_pull_response {} {} data.size() is {} data_included: {}",
      pop.recovery_info, pop.after_progress, data.length(), pop.data_included);
  
  const hobject_t &hoid = pop.soid;
  auto& wfor = recovering[hoid];
  auto& pi = wfor.pi;
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
	pop.recovery_info.copy_subset);
  }

  // If primary doesn't have object info and didn't know version
  if (pi.recovery_info.version == eversion_t())
    pi.recovery_info.version = pop.version;

  bool first = pi.recovery_progress.first;
  auto get_obc_on_first = [=, &pi] {
    if (first) {
      return pg.get_or_load_head_obc(pi.recovery_info.soid).safe_then([=, &pi] (auto p) {
	auto& [obc, existed] = p;
	pi.obc = obc;
	pi.recovery_info.oi = obc->obs.oi;
	return seastar::make_ready_future<>();
      }, crimson::osd::PG::load_obc_ertr::all_same_way(
	  [=, &pi] (const std::error_code& e) {
	  auto [obc, existed] = shard_services.obc_registry.get_cached_obc(pi.recovery_info.soid);
	  pi.obc = obc;
	  return seastar::make_ready_future<>();
	})
      );
    }
    return seastar::make_ready_future<>();
  };

  return get_obc_on_first().then([=, &pi, &pop, data = std::move(data)] () mutable {
    interval_set<uint64_t> usable_intervals;
    ceph::bufferlist usable_data;
    trim_pushed_data(pi.recovery_info.copy_subset, pop.data_included, data,
	&usable_intervals, &usable_data);
    data.claim(usable_data);
    pi.recovery_progress = pop.after_progress;
    logger().debug("new recovery_info {}, new progress {}",
	pi.recovery_info, pi.recovery_progress);
    
    interval_set<uint64_t> data_zeros;
    uint64_t z_offset = pop.before_progress.data_recovered_to;
    uint64_t z_length = pop.after_progress.data_recovered_to
	- pop.before_progress.data_recovered_to;
    if (z_length)
      data_zeros.insert(z_offset, z_length);
    
    bool complete = pi.is_complete();
    bool clear_omap = !pop.before_progress.omap_complete;

    return submit_push_data(pi.recovery_info, first, complete, clear_omap,
	data_zeros, usable_intervals, data, pop.omap_header,
	pop.attrset, pop.omap_entries, t).then(
      [=, &pi, &pop, data = std::move(data)] {
      pi.stat.num_keys_recovered += pop.omap_entries.size();
      pi.stat.num_bytes_recovered += data.length();

      if (complete) {
	pi.stat.num_objects_recovered++;
	pg.on_local_recover(pop.soid, recovering[pop.soid].pi.recovery_info,
			    false, *t);
	return seastar::make_ready_future<bool>(true);
      } else {
	response->soid = pop.soid;
	response->recovery_info = pi.recovery_info;
	response->recovery_progress = pi.recovery_progress;
	return seastar::make_ready_future<bool>(false);
      }
    });
  });
}

seastar::future<> ReplicatedBackend::handle_pull_response(
  const MOSDPGPush& m)
{
  pg_shard_t from = m.from;
  const PushOp& pop = m.pushes[0]; // only one push per message for now
  std::unique_ptr<PullOp> response =
    std::make_unique<PullOp>();
  std::unique_ptr<ceph::os::Transaction> t =
    std::make_unique<ceph::os::Transaction>();

  return _handle_pull_response(from, pop, response.get(), t.get()).then(
    [=, &m, &pop, response = response.get(), t = std::move(t)]
    (bool complete) {
    epoch_t epoch_frozen = pg.get_osdmap_epoch();
    return do_local_transaction(*t).then([=, &m, &pop,
      last_complete = pg.get_info().last_complete] {
      pg._committed_pushed_object(epoch_frozen, last_complete);
      return seastar::make_ready_future<bool>(complete);
    });
  }).then([=, &m, &pop, response = std::move(response)] (bool&& complete) {
    if (complete) {
      recovering[pop.soid].set_pulled();
      return seastar::make_ready_future<>();
    } else {
      auto reply = make_message<MOSDPGPull>();
      reply->from = pg.get_pg_whoami();
      reply->set_priority(m.get_priority());
      reply->pgid = pg.get_info().pgid;
      reply->map_epoch = m.map_epoch;
      reply->min_epoch = m.min_epoch;
      vector<PullOp> vec = { *response };
      reply->set_pulls(&vec);
      return shard_services.send_to_osd(from.osd, std::move(reply), pg.get_osdmap_epoch());
    }
  });
}

seastar::future<> ReplicatedBackend::_handle_push(
  pg_shard_t from,
  const PushOp &pop,
  PushReplyOp *response,
  ceph::os::Transaction *t)
{
  bufferlist data;
  data = pop.data;
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete
    && pop.after_progress.omap_complete;
  bool clear_omap = !pop.before_progress.omap_complete;
  interval_set<uint64_t> data_zeros;
  uint64_t z_offset = pop.before_progress.data_recovered_to;
  uint64_t z_length = pop.after_progress.data_recovered_to
    - pop.before_progress.data_recovered_to;
  if (z_length)
    data_zeros.insert(z_offset, z_length);
  response->soid = pop.recovery_info.soid;
  
  return submit_push_data(pop.recovery_info, first, complete, clear_omap,
      data_zeros, pop.data_included, data, pop.omap_header, pop.attrset,
      pop.omap_entries, t).then([=, &data_zeros] {
    if (complete) {
      pg.on_local_recover(pop.recovery_info.soid,
	  pop.recovery_info, false, *t);
    }
  });
}

seastar::future<> ReplicatedBackend::handle_push(
  const MOSDPGPush& m)
{
  const PushOp& pop = m.pushes[0]; //TODO: only one push per message for now
  std::unique_ptr<PushReplyOp> response =
    std::make_unique<PushReplyOp>();
  std::unique_ptr<ceph::os::Transaction> t =
    std::make_unique<ceph::os::Transaction>();

  return _handle_push(m.from, pop, response.get(), t.get()).then(
    [=, &m, &pop, response = response.get(), t = t.get()] {
    epoch_t epoch_frozen = pg.get_osdmap_epoch();
    return do_local_transaction(*t).then([=, &m, &pop,
      last_complete = pg.get_info().last_complete] {
      //TODO: this should be grouped with pg.on_local_recover somehow.
      pg._committed_pushed_object(epoch_frozen, last_complete);
    });
  }).then([=, &m, &pop, response = std::move(response), t = std::move(t)] {
    auto reply = make_message<MOSDPGPushReply>();
    reply->from = pg.get_pg_whoami();
    reply->set_priority(m.get_priority());
    reply->pgid = pg.get_info().pgid;
    reply->map_epoch = m.map_epoch;
    reply->min_epoch = m.min_epoch;
    std::vector<PushReplyOp> replies = { *response };
    reply->replies.swap(replies);
    return shard_services.send_to_osd(m.from.osd,
	std::move(reply), pg.get_osdmap_epoch());
  });
}

seastar::future<bool> ReplicatedBackend::_handle_push_reply(
  pg_shard_t peer,
  const PushReplyOp &op,
  PushOp *reply)
{
  const hobject_t& soid = op.soid;
  auto recovering_iter = recovering.find(soid);
  if (recovering_iter == recovering.end()
      || recovering_iter->second.pushing.count(peer)) {
    logger().debug("huh, i wasn't pushing {} to osd.{}", soid, peer);
    return seastar::make_ready_future<bool>(true);
  } else {
    auto& pi = recovering_iter->second.pushing[peer];

    auto f = [=, &pi, &soid] {
      bool error = pi.recovery_progress.error;
      if (!pi.recovery_progress.data_complete && !error) {
	return build_push_op(pi.recovery_info, pi.recovery_progress,
	    &pi.stat, reply).then([&pi] (auto new_progress) {
	  pi.recovery_progress = new_progress;
	  return seastar::make_ready_future<bool>(false);
	});
      }
      if (!error)
	pg.on_peer_recover(peer, soid, pi.recovery_info);
      recovering_iter->second.set_pushed(peer);
      return seastar::make_ready_future<bool>(true);
    };
    
    return f().handle_exception([=, &pi, &soid] (auto e) {
      pi.recovery_progress.error = true;
      recovering_iter->second.set_pushed(peer);
      return seastar::make_ready_future<bool>(true);
    });
  }
}

seastar::future<> ReplicatedBackend::handle_push_reply(
  const MOSDPGPushReply& m)
{
  auto from = m.from;
  auto& push_reply = m.replies[0]; //TODO: only one reply per message
  std::unique_ptr<PushOp> pop =
    std::make_unique<PushOp>();

  return _handle_push_reply(from, push_reply, pop.get()).then(
    [=, &push_reply, pop = std::move(pop)] (bool finished) {
    if (!finished) {
      auto msg = make_message<MOSDPGPush>();
      msg->from = pg.get_pg_whoami();
      msg->pgid = pg.get_pgid();
      msg->map_epoch = pg.get_osdmap_epoch();
      msg->min_epoch = pg.get_last_peering_reset();
      msg->set_priority(pg.get_recovery_op_priority());
      msg->pushes.push_back(*pop);
      return shard_services.send_to_osd(from.osd, std::move(msg), pg.get_osdmap_epoch());
    }
    return seastar::make_ready_future<>();
  });
}

void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  ceph::bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset, intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
      p != intervals_received.end(); ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin(); q != x.end();
	++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

seastar::future<> ReplicatedBackend::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  bool clear_omap,
  interval_set<uint64_t> &data_zeros,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  const map<string, bufferlist> &attrs,
  const map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  hobject_t target_oid;
  if (first && complete) {
    target_oid = recovery_info.soid;
  } else {
    target_oid = get_temp_recovery_object(recovery_info.soid,
					  recovery_info.version);
    if (first) {
      logger().debug("{}: Adding oid {} in the temp collection",
	  __func__, target_oid);
      add_temp_obj(target_oid);
    }
  }

  auto on_first = [=, &recovery_info, &data_zeros, &intervals_included,
    &omap_header, &attrs, &omap_entries] {
    if (first) {
      if (!complete) {
	t->remove(coll->get_cid(), ghobject_t(target_oid));
	t->touch(coll->get_cid(), ghobject_t(target_oid));
	bufferlist bv = attrs.at(OI_ATTR);
	object_info_t oi(bv);
	t->set_alloc_hint(coll->get_cid(), ghobject_t(target_oid),
			  oi.expected_object_size,
			  oi.expected_write_size,
			  oi.alloc_hint_flags);
      } else {
        if (!recovery_info.object_exist) {
	  t->remove(coll->get_cid(), ghobject_t(target_oid));
          t->touch(coll->get_cid(), ghobject_t(target_oid));
          bufferlist bv = attrs.at(OI_ATTR);
          object_info_t oi(bv);
          t->set_alloc_hint(coll->get_cid(), ghobject_t(target_oid),
                            oi.expected_object_size,
                            oi.expected_write_size,
                            oi.alloc_hint_flags);
        }
        //remove xattr and update later if overwrite on original object
        t->rmattrs(coll->get_cid(), ghobject_t(target_oid));
        //if need update omap, clear the previous content first
        if (clear_omap)
          t->omap_clear(coll->get_cid(), ghobject_t(target_oid));
      }

      t->truncate(coll->get_cid(), ghobject_t(target_oid), recovery_info.size);
      if (omap_header.length())
	t->omap_setheader(coll->get_cid(), ghobject_t(target_oid), omap_header);

      return store->stat(coll, ghobject_t(recovery_info.soid)).then (
	[=, &recovery_info, &data_zeros, &intervals_included,
	omap_header = std::move(omap_header), &attrs, &omap_entries] (auto st) {
	//TODO: pg num bytes counting
	if (!complete) {
	  //clone overlap content in local object
	  if (recovery_info.object_exist) {
	    assert(r == 0);
	    uint64_t local_size = std::min(recovery_info.size, (uint64_t)st.st_size);
	    interval_set<uint64_t> local_intervals_included, local_intervals_excluded;
	    if (local_size) {
	      local_intervals_included.insert(0, local_size);
	      local_intervals_excluded.intersection_of(local_intervals_included, recovery_info.copy_subset);
	      local_intervals_included.subtract(local_intervals_excluded);
	    }
	    for (interval_set<uint64_t>::const_iterator q = local_intervals_included.begin();
		q != local_intervals_included.end();
		++q) {
	      logger().debug(" clone_range {} {}~{}",
		  recovery_info.soid, q.get_start(), q.get_len());
	      t->clone_range(coll->get_cid(), ghobject_t(recovery_info.soid), ghobject_t(target_oid),
		  q.get_start(), q.get_len(), q.get_start());
	    }
	  }
	}
	return seastar::make_ready_future<>();
      });
    }
    return seastar::make_ready_future<>();
  };


  return on_first().then([=, &data_zeros, &recovery_info, &intervals_included,
    &omap_entries, &attrs] {
    uint64_t off = 0;
    uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL;
    // Punch zeros for data, if fiemap indicates nothing but it is marked dirty
    if (data_zeros.size() > 0) {
      data_zeros.intersection_of(recovery_info.copy_subset);
      assert(intervals_included.subset_of(data_zeros));
      data_zeros.subtract(intervals_included);

      logger().debug("{} recovering object {} copy_subset: {} "
	  "intervals_included: {} data_zeros: {}",
	  recovery_info.soid, recovery_info.copy_subset,
	  intervals_included, data_zeros);

      for (auto p = data_zeros.begin(); p != data_zeros.end(); ++p)
	t->zero(coll->get_cid(), ghobject_t(target_oid), p.get_start(), p.get_len());
    }
    for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
	p != intervals_included.end();
	++p) {
      bufferlist bit;
      bit.substr_of(data_included, off, p.get_len());
      t->write(coll->get_cid(), ghobject_t(target_oid),
	  p.get_start(), p.get_len(), bit, fadvise_flags);
      off += p.get_len();
    }

    if (!omap_entries.empty())
      t->omap_setkeys(coll->get_cid(), ghobject_t(target_oid), omap_entries);
    if (!attrs.empty())
      t->setattrs(coll->get_cid(), ghobject_t(target_oid), attrs);

    if (complete) {
      if (!first) {
	logger().debug("{}: Removing oid {} from the temp collection",
	    __func__, target_oid);
	clear_temp_obj(target_oid);
	t->remove(coll->get_cid(), ghobject_t(recovery_info.soid));
	t->collection_move_rename(coll->get_cid(), ghobject_t(target_oid),
				  coll->get_cid(), ghobject_t(recovery_info.soid));
      }
      submit_push_complete(recovery_info, t);
    }
    return seastar::make_ready_future<>();
  });
}

void ReplicatedBackend::submit_push_complete(
  const ObjectRecoveryInfo &recovery_info,
  ObjectStore::Transaction *t
  ) {
  for (map<hobject_t, interval_set<uint64_t>>::const_iterator p =
      recovery_info.clone_subset.begin();
      p != recovery_info.clone_subset.end(); ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	q != p->second.end(); ++q) {
      logger().debug(" clone_range {} {}~{}", p->first, q.get_start(), q.get_len());
      t->clone_range(coll->get_cid(), ghobject_t(p->first), ghobject_t(recovery_info.soid),
	  q.get_start(), q.get_len(), q.get_start());
    }
  }
}
