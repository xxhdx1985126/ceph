void Objecter::handle_osd_map(MOSDMap *m) {
  ceph::shunique_lock sul(rwlock, acquire_unique);
  if (!initialized)
    return;

  ceph_assert(osdmap);

  if (m->fsid != monc->get_fsid()) {
    ldout(cct, 0) << "handle_osd_map fsid " << m->fsid
                  << " != " << monc->get_fsid() << dendl;
    return;
  }

  // +++ 新增：保存旧的 OSDMap 用于比较 +++
  std::unique_ptr<OSDMap> old_osdmap;
  old_osdmap = std::make_unique<OSDMap>(*osdmap); // 深拷贝当前OSDMap
  // 用于记录需要更新缓存的PG列表
  std::set<pg_t> pgs_in_cache = metadata_cacher.get_pgids();

  bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool cluster_full = _osdmap_full_flag();
  bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || cluster_full ||
                     _osdmap_has_pool_full();
  map<int64_t, bool> pool_full_map;
  for (auto it = osdmap->get_pools().begin(); it != osdmap->get_pools().end();
       ++it)
    pool_full_map[it->first] = _osdmap_pool_full(it->second);

  list<LingerOp *> need_resend_linger;
  map<ceph_tid_t, Op *> need_resend;
  map<ceph_tid_t, CommandOp *> need_resend_command;

  if (m->get_last() <= osdmap->get_epoch()) {
    ldout(cct, 3) << "handle_osd_map ignoring epochs [" << m->get_first() << ","
                  << m->get_last() << "] <= " << osdmap->get_epoch() << dendl;
  } else {
    ldout(cct, 3) << "handle_osd_map got epochs [" << m->get_first() << ","
                  << m->get_last() << "] > " << osdmap->get_epoch() << dendl;

    if (osdmap->get_epoch()) {
      bool skipped_map = false;
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1; e <= m->get_last(); e++) {
        // 对osdmap的增量更新和全量更新
        if (osdmap->get_epoch() == e - 1 && m->incremental_maps.count(e)) {
          ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e
                        << dendl;
          OSDMap::Incremental inc(m->incremental_maps[e]);
          osdmap->apply_incremental(inc);

          emit_blocklist_events(inc);

          logger->inc(l_osdc_map_inc);
        } else if (m->maps.count(e)) {
          ldout(cct, 3) << "handle_osd_map decoding full epoch " << e << dendl;
          auto new_osdmap = std::make_unique<OSDMap>();
          new_osdmap->decode(m->maps[e]);

          emit_blocklist_events(*osdmap, *new_osdmap);
          osdmap = std::move(new_osdmap);

          logger->inc(l_osdc_map_full);
        } else {
          if (e >= m->cluster_osdmap_trim_lower_bound) {
            ldout(cct, 3) << "handle_osd_map requesting missing epoch "
                          << osdmap->get_epoch() + 1 << dendl;
            _maybe_request_map();
            break;
          }
          ldout(cct, 3) << "handle_osd_map missing epoch "
                        << osdmap->get_epoch() + 1 << ", jumping to "
                        << m->cluster_osdmap_trim_lower_bound << dendl;
          e = m->cluster_osdmap_trim_lower_bound - 1;
          skipped_map = true;
          continue;
        }
        logger->set(l_osdc_map_epoch, osdmap->get_epoch());

        prune_pg_mapping(osdmap->get_pools());
        cluster_full = cluster_full || _osdmap_full_flag();
        update_pool_full_map(pool_full_map);

        // check all outstanding requests on every epoch
        for (auto &i : need_resend) {
          _prune_snapc(osdmap->get_new_removed_snaps(), i.second);
        }
        _scan_requests(homeless_session, skipped_map, cluster_full,
                       &pool_full_map, need_resend, need_resend_linger,
                       need_resend_command, sul);
        for (auto p = osd_sessions.begin(); p != osd_sessions.end();) {
          auto s = p->second;
          _scan_requests(s, skipped_map, cluster_full, &pool_full_map,
                         need_resend, need_resend_linger, need_resend_command,
                         sul);
          ++p;
          // osd down or addr change?
          if (!osdmap->is_up(s->osd) ||
              (s->con &&
               s->con->get_peer_addrs() != osdmap->get_addrs(s->osd))) {
            close_session(s);
            // osd变化了导致pg映射发生变化
          }
        }

        ceph_assert(e == osdmap->get_epoch());
      }

    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
        for (auto p = osd_sessions.begin(); p != osd_sessions.end(); ++p) {
          OSDSession *s = p->second;
          _scan_requests(s, false, false, NULL, need_resend, need_resend_linger,
                         need_resend_command, sul);
        }
        ldout(cct, 3) << "handle_osd_map decoding full epoch " << m->get_last()
                      << dendl;
        osdmap->decode(m->maps[m->get_last()]);

        

        prune_pg_mapping(osdmap->get_pools());

        _scan_requests(homeless_session, false, false, NULL, need_resend,
                       need_resend_linger, need_resend_command, sul);
      } else {
        ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting"
                      << dendl;
        monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
        monc->renew_subs();
      }
    }
  }
  // +++对于第一次全量Map，检查所有PG 
  bool pg_changed;
  if (old_osdmap) {
    pg_changed = _check_pg_acting_set_changes(*old_osdmap, *osdmap,
                                  pgs_with_acting_changes);
  }

  // make sure need_resend targets reflect latest map
  for (auto p = need_resend.begin(); p != need_resend.end();) {
    Op *op = p->second;
    if (op->target.epoch < osdmap->get_epoch()) {
      ldout(cct, 10) << __func__ << "  checking op " << p->first << dendl;
      int r = _calc_target(&op->target, nullptr);
      if (r == RECALC_OP_TARGET_POOL_DNE) {
        p = need_resend.erase(p);
        _check_op_pool_dne(op, nullptr);
      } else {
        ++p;
      }
    } else {
      ++p;
    }
  }

  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) ||
                 _osdmap_full_flag() || _osdmap_has_pool_full();

  // was/is paused?
  if (was_pauserd || was_pausewr || pauserd || pausewr ||
      osdmap->get_epoch() < epoch_barrier) {
    _maybe_request_map();
  }

  // resend requests
  for (auto p = need_resend.begin(); p != need_resend.end(); ++p) {
    auto op = p->second;
    auto s = op->session;
    bool mapped_session = false;
    if (!s) {
      int r = _map_session(&op->target, &s, sul);
      ceph_assert(r == 0);
      mapped_session = true;
    } else {
      get_session(s);
    }
    std::unique_lock sl(s->lock);
    if (mapped_session) {
      _session_op_assign(s, op);
    }
    if (op->should_resend) {
      if (!op->session->is_homeless() && !op->target.paused) {
        logger->inc(l_osdc_op_resend);
        _send_op(op);
      }
    } else {
      _op_cancel_map_check(op);
      _cancel_linger_op(op);
    }
    sl.unlock();
    put_session(s);
  }
  for (auto p = need_resend_linger.begin(); p != need_resend_linger.end();
       ++p) {
    LingerOp *op = *p;
    ceph_assert(op->session);
    if (!op->session->is_homeless()) {
      logger->inc(l_osdc_linger_resend);
      _send_linger(op, sul);
    }
  }
  for (auto p = need_resend_command.begin(); p != need_resend_command.end();
       ++p) {
    auto c = p->second;
    if (c->target.osd >= 0) {
      _assign_command_session(c, sul);
      if (c->session && !c->session->is_homeless()) {
        _send_command(c);
      }
    }
  }

  _dump_active();

  // finish any Contexts that were waiting on a map update
  auto p = waiting_for_map.begin();
  while (p != waiting_for_map.end() && p->first <= osdmap->get_epoch()) {
    // go through the list and call the onfinish methods
    for (auto &[c, ec] : p->second) {
      asio::post(service.get_executor(), asio::append(std::move(c), ec));
    }
    p = waiting_for_map.erase(p);
  }

  monc->sub_got("osdmap", osdmap->get_epoch());

  if (!waiting_for_map.empty()) {
    _maybe_request_map();
  }
}

// +++ 检查PG acting set变化 +++
bool Objecter::_check_pg_acting_set_changes(const OSDMap& old_osdmap,
                                            const OSDMap& new_osdmap,
                                            std::set<pg_t>& pgid_in_cache) {
  // 获取所有pool的PG数量
  const auto &pools = new_osdmap.get_pools();
  set<pg_t> pg_changed;
  for (auto pgid : pgid_in_cache) {
    vector<int> old_up, old_acting;
    int old_up_primary, old_acting_primary;
    old_osdmap.pg_to_up_acting_osds(pgid, &old_up, &old_up_primary,
                                    &old_acting, &old_acting_primary);

    vector<int> new_up, new_acting;
    int new_up_primary, new_acting_primary;
    new_osdmap.pg_to_up_acting_osds(pgid, &new_up, &new_up_primary,
                                    &new_acting, &new_acting_primary);

    // 比较acting set是否变化
    if (old_acting != new_acting) {
      pg_changed.insert(pgid);
    }
  }
  metadata_cacher.pgs_remove(pg_changed);
}


