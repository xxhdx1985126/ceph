// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "messages/MOSDOpReply.h"

#define dout_subsys ceph_subsys_ms

void MOSDOpReply::decode_payload() {
  using ceph::decode;
  auto p = payload.cbegin();

  // Always keep here the newest version of decoding order/rule
  if (header.version == HEAD_VERSION) {
    decode(oid, p);
    decode(pgid, p);
    decode(flags, p);
    decode(result, p);
    decode(bad_replay_version, p);
    decode(osdmap_epoch, p);

    __u32 num_ops = ops.size();
    decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      decode(ops[i].op, p);
    decode(retry_attempt, p);

    for (unsigned i = 0; i < num_ops; ++i)
      decode(ops[i].rval, p);

    OSDOp::split_osd_op_vector_out_data(ops, data);

    decode(replay_version, p);
    decode(user_version, p);
    decode(do_redirect, p);
    if (do_redirect)
      decode(redirect, p);
    decode_trace(p);

    decode(has_target_cache_data, p);
    decode(has_head_cache_data, p);
    decode(target_cached_data, p);
    decode(head_cached_data, p);
    // target_cached_data.decode(p);
    // head_cached_data.decode(p);
    decode(ss, p);
  } else if (header.version < 2) {
    ceph_osd_reply_head head;
    decode(head, p);
    ops.resize(head.num_ops);
    for (unsigned i = 0; i < head.num_ops; i++) {
      decode(ops[i].op, p);
    }
    ceph::decode_nohead(head.object_len, oid.name, p);
    pgid = pg_t(head.layout.ol_pgid);
    result = (int32_t)head.result;
    flags = head.flags;
    replay_version = head.reassert_version;
    user_version = replay_version.version;
    osdmap_epoch = head.osdmap_epoch;
    retry_attempt = -1;
  } else {
    decode(oid, p);
    decode(pgid, p);
    decode(flags, p);
    decode(result, p);
    decode(bad_replay_version, p);
    decode(osdmap_epoch, p);

    __u32 num_ops = ops.size();
    decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      decode(ops[i].op, p);

    if (header.version >= 3)
      decode(retry_attempt, p);
    else
      retry_attempt = -1;

    if (header.version >= 4) {
      for (unsigned i = 0; i < num_ops; ++i)
	decode(ops[i].rval, p);

      OSDOp::split_osd_op_vector_out_data(ops, data);
    }

    if (header.version >= 5) {
      decode(replay_version, p);
      decode(user_version, p);
    } else {
      replay_version = bad_replay_version;
      user_version = replay_version.version;
    }

    if (header.version == 6) {
      decode(redirect, p);
      do_redirect = !redirect.empty();
    }
    if (header.version >= 7) {
      decode(do_redirect, p);
      if (do_redirect) {
	decode(redirect, p);
      }
    }
    if (header.version >= 8) {
      decode_trace(p);
    }

    decode(has_target_cache_data, p);
    decode(has_head_cache_data, p);
    decode(target_cached_data, p);
    decode(head_cached_data, p);
    // target_cached_data.decode(p);
    // head_cached_data.decode(p);
    decode(ss, p);
  }
}
