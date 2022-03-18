// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/backref/btree_backref_manager.h"

SET_SUBSYS(seastore_backref);

namespace crimson::os::seastore {

template<>
Transaction::tree_stats_t& get_tree_stats<
  crimson::os::seastore::backref::BackrefBtree>(Transaction &t) {
  return t.get_backref_tree_stats();
}

template<>
phy_tree_root_t& get_phy_tree_root<
  crimson::os::seastore::backref::BackrefBtree>(root_t &r) {
  return r.backref_root;
}

}

namespace crimson::os::seastore::backref {

BtreeBackrefManager::mkfs_ret
BtreeBackrefManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeBackrefManager::mkfs);
  INFOT("start", t);
  return cache.get_root(t).si_then([this, &t](auto croot) {
    croot->get_root().backref_root = BackrefBtree::mkfs(get_context(t));
    return mkfs_iertr::now();
  }).handle_error_interruptible(
    mkfs_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BtreeBackrefManager::mkfs"
    }
  );
}

BtreeBackrefManager::get_mapping_ret
BtreeBackrefManager::get_mapping(
  Transaction &t,
  paddr_t offset)
{
  LOG_PREFIX(BtreeBackrefManager::get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  return with_btree_ret<BackrefBtree, BackrefPinRef>(
    cache,
    c,
    [c, offset](auto &btree) {
    return btree.lower_bound(
      c, offset
    ).si_then([offset, c](auto iter) -> get_mapping_ret {
      LOG_PREFIX(BtreeBackrefManager::get_mapping);
      if (iter.is_end() || iter.get_key() != offset) {
	DEBUGT("{} doesn't exist", c.trans, offset);
	return crimson::ct_error::enoent::make();
      } else {
	TRACET("{} got {}, {}",
	       c.trans, offset, iter.get_key(), iter.get_val());
	auto e = iter.get_pin();
	return get_mapping_ret(
	  interruptible::ready_future_marker{},
	  std::move(e));
      }
    });
  });
}

BtreeBackrefManager::get_mappings_ret
BtreeBackrefManager::get_mappings(
  Transaction &t,
  paddr_t offset,
  extent_len_t length)
{
  LOG_PREFIX(BtreeBackrefManager::get_mappings);
  TRACET("{}~{}", t, offset, length);
  auto c = get_context(t);
  return with_btree_state<BackrefBtree, backref_pin_list_t>(
    cache,
    c,
    [c, offset, length](auto &btree, auto &ret) {
      return BackrefBtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, offset),
	[&ret, offset, length, c](auto &pos) {
	  LOG_PREFIX(BtreeBackrefManager::get_mappings);
	  if (pos.is_end() || pos.get_key() >= (offset.add_offset(length))) {
	    TRACET("{}~{} done with {} results",
	           c.trans, offset, length, ret.size());
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  TRACET("{}~{} got {}, {}, repeat ...",
	         c.trans, offset, length, pos.get_key(), pos.get_val());
	  ceph_assert((pos.get_key() + pos.get_val().len) > offset);
	  ret.push_back(pos.get_pin());
	  return BackrefBtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	});
    });
}

BtreeBackrefManager::new_mapping_ret
BtreeBackrefManager::new_mapping(
  Transaction &t,
  paddr_t key,
  extent_len_t len,
  laddr_t addr,
  extent_types_t type)
{
  ceph_assert(
    is_aligned(
      key.as_seg_paddr().get_segment_off(),
      (uint64_t)segment_manager.get_block_size()));
  struct state_t {
    paddr_t last_end;

    std::optional<BackrefBtree::iterator> insert_iter;
    std::optional<BackrefBtree::iterator> ret;

    state_t(paddr_t hint) : last_end(hint) {}
  };

  LOG_PREFIX(BtreeBackrefManager::alloc_extent);
  DEBUGT("{}~{}, paddr={}", t, addr, len, key);
  backref_map_val_t val{len, addr, type};
  auto c = get_context(t);
  //++stats.num_alloc_extents;
  //auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return crimson::os::seastore::with_btree_state<BackrefBtree, state_t>(
    cache,
    c,
    key,
    [val, c, key, len, addr, /*lookup_attempts,*/ &t]
    (auto &btree, auto &state) {
      return BackrefBtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, key),
	[&state, len, addr, &t, key/*, lookup_attempts*/](auto &pos) {
	  LOG_PREFIX(BtreeBackrefManager::new_mapping);
	  //++stats.num_alloc_extents_iter_nexts;
	  if (pos.is_end()) {
	    DEBUGT("{}~{}, paddr={}, state: end, insert at {}",
                   t, addr, len, key,
                   //stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end);
	    state.insert_iter = pos;
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else if (pos.get_key() >= (state.last_end.add_offset(len))) {
	    DEBUGT("{}~{}, paddr={}, state: {}~{}, "
		   "insert at {} -- {}",
                   t, addr, len, key,
                   pos.get_key(), pos.get_val().len,
                   //stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end,
                   pos.get_val());
	    state.insert_iter = pos;
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else {
	    DEBUGT("{}~{}, paddr={}, state: {}~{}, repeat ... -- {}",
                   t, addr, len, key,
                   pos.get_key(), pos.get_val().len,
                   pos.get_val());
	    ceph_abort("not possible for the backref tree");
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([c, addr, len, key, &btree, &state, val] {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    val
	  ).si_then([&state, c, addr, len, key](auto &&p) {
	    LOG_PREFIX(BtreeBackrefManager::alloc_extent);
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, paddr={}, inserted at {}",
	           c.trans, addr, len, key, state.last_end);
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([](auto &&state) {
      return state.ret->get_pin();
    });
}

BtreeBackrefManager::batch_insert_ret
BtreeBackrefManager::batch_insert_from_cache(
  Transaction &t,
  const journal_seq_t &limit)
{
  LOG_PREFIX(BtreeBackrefManager::batch_insert_from_cache);
  DEBUGT("insert up to {}", t, limit);
  return seastar::do_with(
    limit,
    [this, &t](auto &limit) {
    return trans_intr::do_for_each(
      cache.get_backref_bufs_to_flush(),
      [this, &limit, &t](auto &bbr) -> batch_insert_iertr::future<> {
      LOG_PREFIX(BtreeBackrefManager::batch_insert_from_cache);
      DEBUGT("backref buffer starting seq: {}", t, bbr->backrefs.begin()->first);
      if (bbr->backrefs.begin()->first <= limit) {
	return batch_insert(
	  t,
	  bbr,
	  limit);
      }
      return seastar::now();
    });
  });
}

BtreeBackrefManager::scan_mapped_space_ret
BtreeBackrefManager::scan_mapped_space(
  Transaction &t,
  BtreeBackrefManager::scan_mapped_space_func_t &&f)
{
  LOG_PREFIX(BtreeBackrefManager::scan_mapped_space);
  DEBUGT("start", t);
  auto c = get_context(t);
  return seastar::do_with(
    std::move(f),
    [this, c](auto &visitor) {
      return with_btree<BackrefBtree>(
	cache,
	c,
	[c, &visitor](auto &btree) {
	  return BackrefBtree::iterate_repeat(
	    c,
	    btree.lower_bound(
	      c,
	      paddr_t::make_seg_paddr(
		segment_id_t{0, 0}, 0),
	      &visitor),
	    [&visitor](auto &pos) {
	      if (pos.is_end()) {
		return BackrefBtree::iterate_repeat_ret_inner(
		  interruptible::ready_future_marker{},
		  seastar::stop_iteration::yes);
	      }
	      visitor(pos.get_key(), pos.get_val().len, 0);
	      return BackrefBtree::iterate_repeat_ret_inner(
		interruptible::ready_future_marker{},
		seastar::stop_iteration::no);
	    },
	    &visitor);
	});
    });
}

BtreeBackrefManager::batch_insert_ret
BtreeBackrefManager::batch_insert(
  Transaction &t,
  backref_buffer_ref &bbr,
  const journal_seq_t &limit)
{
  return trans_intr::do_for_each(bbr->backrefs,
    [this, &t, &limit](auto &p) -> batch_insert_iertr::future<> {
    auto &seq = p.first;
    auto &backref_list = p.second;
    LOG_PREFIX(BtreeBackrefManager::batch_insert);
    DEBUGT("seq {}, limit {}", t, seq, limit);
    if (seq <= limit) {
      return trans_intr::do_for_each(
	backref_list,
	[this, &t](auto &backref) {
	LOG_PREFIX(BtreeBackrefManager::batch_insert);
	if (backref->laddr != L_ADDR_NULL) {
	  DEBUGT("new mapping: {}~{} -> {}",
	    t, backref->paddr, backref->len, backref->laddr);
	  return new_mapping(
	    t,
	    backref->paddr,
	    backref->len,
	    backref->laddr,
	    backref->type).si_then([](auto &&pin) {
	    return seastar::now();
	  });
	} else {
	  DEBUGT("remove mapping: {}", t, backref->paddr);
	  return remove_mapping(
	    t,
	    backref->paddr).si_then([](auto&&) {
	    return seastar::now();
	  }).handle_error_interruptible(
	    crimson::ct_error::input_output_error::pass_further(),
	    crimson::ct_error::assert_all("no enoent possible")
	  );
	}
      });
    }
    return seastar::now();
  });
}

BtreeBackrefManager::rewrite_extent_ret
BtreeBackrefManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  auto c = get_context(t);
  return with_btree<BackrefBtree>(
    cache,
    c,
    [c, extent](auto &btree) mutable {
    return btree.rewrite_extent(c, extent);
  });
}

BtreeBackrefManager::remove_mapping_ret
BtreeBackrefManager::remove_mapping(
  Transaction &t,
  paddr_t addr)
{
  auto c = get_context(t);
  return with_btree_ret<BackrefBtree, remove_mapping_result_t>(
    cache,
    c,
    [c, addr](auto &btree) mutable {
      return btree.lower_bound(
	c, addr
      ).si_then([&btree, c, addr](auto iter)
		-> remove_mapping_ret {
	if (iter.is_end() || iter.get_key() != addr) {
	  LOG_PREFIX(BtreeBackrefManager::_update_mapping);
	  DEBUGT("laddr={} doesn't exist", c.trans, addr);
	  return crimson::ct_error::enoent::make();
	}

	auto ret = remove_mapping_result_t{
	  iter.get_key(),
	  iter.get_val().len,
	  iter.get_val().laddr};
	return btree.remove(
	  c,
	  iter
	).si_then([ret] {
	  return ret;
	});
      });
    });
}

} // namespace crimson::os::seastore::backref
