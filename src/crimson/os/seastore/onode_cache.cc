// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/onode_cache.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "seastar/core/metrics.hh"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

OnodeCache::OnodeCache()
  : onode_reservation_length(
      crimson::common::get_conf<uint64_t>(
	"seastore_default_max_object_size")),
    onode_cache_memory_capacity(104857600),
    evict_size_per_cycle(2097152),
    cached_extents_size_limit(2097152) {
  register_metrics();
}

OnodeCache::~OnodeCache() {}

auto OnodeCache::find_laddr(laddr_t laddr)
{
  // auto p = laddr_set.lower_bound(laddr);
  // if (p != laddr_set.begin() &&
  //     (p == laddr_set.end() ||
  //      p->laddr > laddr)) {
  //   --p;
  //   if (p->laddr + onode_reservation_length <= laddr) {
  //     ++p;
  //   }
  // }
  // return std::make_pair(
  //   p,
  //   p != laddr_set.end() &&
  //   p->laddr <= laddr &&
  //   p->laddr + onode_reservation_length > laddr);
  auto p = laddr_set.find(laddr);
  return std::make_pair(p, p != laddr_set.end());
}

void OnodeCache::touch(laddr_t laddr)
{
  stats.object_data_block_counts++;
  auto p = find_laddr(laddr);
  if (!p.second) {
    auto e = create_entry(laddr);
    move_to_top(*e);
  }
}

void OnodeCache::add(laddr_t laddr) {
  auto p = find_laddr(laddr);
  if (p.second) {
    auto &e = const_cast<entry_t&>(*p.first);
    move_to_top(e);
  } else {
    auto e = create_entry(laddr);
    move_to_top(*e);
  }
}

void OnodeCache::remove(laddr_t laddr)
{
  auto p = find_laddr(laddr);
  if (p.second) {
    entry_ref_t e = const_cast<entry_t*>(&*p.first);
    remove_from_lru(*e);
    release_entry(e);
  }
}

void OnodeCache::remove_direct(laddr_t laddr) {
  auto p = laddr_set.find(laddr);
  ceph_assert(p != laddr_set.end());
  entry_ref_t e = const_cast<entry_t*>(&*p);
  remove_from_lru(*e);
  release_entry(e);
}

bool OnodeCache::should_evict() const
{
  return (laddr_set.size() * sizeof(entry_t) * 2) > onode_cache_memory_capacity;
}

OnodeCache::entry_ref_t OnodeCache::create_entry(laddr_t laddr)
{
  auto e = entry_ref_t(new entry_t(laddr));
  intrusive_ptr_add_ref(&*e);
  laddr_set.insert(*e);
  if (should_evict()) {
    background_callback->maybe_wake_background();
  }
  stats.onode_counts++;
  return e;
}

void OnodeCache::add_extent(CachedExtentRef extent)
{
  if (should_write_out_extents()) {
    background_callback->maybe_wake_write_cache();
    return;
  }
  if (extent->is_in_read_cache()) {
    return;
  }
  intrusive_ptr_add_ref(&*extent);
  read_cache.push_back(*extent);
  read_size += extent->get_length();
  added_size += extent->get_length();
  read_cache_paddr.insert(extent->get_paddr());
}

void OnodeCache::remove_extent(CachedExtentRef extent)
{
  if (extent->is_in_read_cache()) {
    if (read_cache_paddr.contains(extent->get_paddr())) {
      read_cache.erase(read_cache.s_iterator_to(*extent));
      read_cache_paddr.erase(extent->get_paddr());
    } else {
      pending_write.erase(pending_write.s_iterator_to(*extent));
    }
    read_size -= extent->get_length();
    retired_size += extent->get_length();
    intrusive_ptr_release(&*extent);
  }
}

std::list<CachedExtentRef> OnodeCache::get_cached_extents(Transaction &t)
{
  pending_write.splice(pending_write.end(), read_cache);
  read_cache_paddr.clear();
  std::list<CachedExtentRef> res;
  std::list<CachedExtentRef> invalidated_extents;
  for (auto &ext : pending_write) {
    if (ext.is_valid() && ext.is_clean()) {
      t.add_to_read_set(&ext);
      res.emplace_back(&ext);
    } else {
      invalidated_extents.emplace_back(&ext);
    }
  }
  for (auto &ext : invalidated_extents) {
    remove_extent(ext);
  }
  return res;
}

void OnodeCache::reset_cached_extents()
{
  for (auto it = pending_write.begin(); it != pending_write.end();) {
    auto &ext = *it;
    it = pending_write.erase(it);
    read_size -= ext.get_length();
    intrusive_ptr_release(&ext);
  }
  assert(pending_write.empty());
}

seastar::future<> OnodeCache::write_cache()
{
  LOG_PREFIX(OnodeCache::write_cache);
  return repeat_eagain([this, FNAME] {
    return tm->with_transaction_intr(Transaction::src_t::WRITE_CACHE, "write_cache", [this, FNAME](auto &t) {
      return seastar::do_with(get_cached_extents(t), seastar::lowres_system_clock::now(), (std::size_t)0,
			      [this, &t, FNAME](auto &list, auto& mtime, auto& size) {
	INFO("write {} cached exents", list.size());
	return trans_intr::do_for_each(list, [this, &t, &mtime, &size](auto ext) {
	  size += ext->get_length();
	  ext->set_user_hint(placement_hint_t::READ_CACHE);
	  return tm->rewrite_extent(t, ext, MIN_REWRITE_GENERATION, mtime);
        }).si_then([this, &t] {
	  return tm->submit_transaction_direct(t);
	}).si_then([this, &t, &size, &list] {
	  INFO("write {}bytes cached exents from read", size);
	  stats.object_data_block_counts += list.size();
	  stats.read_counts += list.size();
	  stats.read_size += size;
	});
      });
    });
  }).handle_error(crimson::ct_error::assert_all{ "impossible" });
}

seastar::future<> OnodeCache::evict()
{
  LOG_PREFIX(OnodeCache::evict);
  INFO("start");
  return repeat_eagain([this, FNAME] {
    evict_state.reset();
    {
      auto iter = lru.begin();
      for (int i = 0; i < 20 && iter != lru.end(); i++) {
	evict_state.cold_onode.push_back(iter->laddr);
	iter++;
      }
      evict_state.cold_onode_cursor = evict_state.cold_onode.begin();
    }
    DEBUG("start {}", evict_state);
    return tm->with_transaction_intr(
	Transaction::src_t::EVICT,
	"evict",
	[this, FNAME](auto &t) {
      return trans_intr::repeat([this, &t, FNAME] {
	return tm->get_pins(
	  t, *evict_state.cold_onode_cursor, onode_reservation_length
	).si_then([this, &t, FNAME](auto pins) {
	  auto processed_pin_size = 0;
	  for (auto iter = pins.begin(); iter != pins.end(); ++iter) {
	    auto &pin = *iter;
	    if (evict_state.write_size < evict_size_per_cycle) {
	      TRACE("consume one pin, {}", evict_state);
	      processed_pin_size++;
	      auto paddr = pin->get_val();
	      if (paddr.is_absolute() &&
		  epm->is_hot_device(paddr.get_device_id())) {
		evict_state.write_size += pin->get_length();
		evict_state.cold_pins_list.emplace_back(std::move(*iter));
	      }
	    } else {
	      break;
	    }
	  }

	  TRACE("consume one onode, {}", evict_state);
	  if (processed_pin_size == pins.size()) {
	    evict_state.completed.push_back(*evict_state.cold_onode_cursor);
	  }
	  evict_state.cold_onode_cursor++;
	  if (evict_state.write_size >= evict_size_per_cycle ||
	      evict_state.cold_onode_cursor == evict_state.cold_onode.end()) {
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	  } else {
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no);
	  }
	});
      }).si_then([this, &t, FNAME] {
	return trans_intr::parallel_for_each(evict_state.cold_pins_list,
					     [this, &t, FNAME](auto &pin) {
	  return tm->pin_to_extent_by_type(
	      t, pin->duplicate(), extent_types_t::OBJECT_DATA_BLOCK
	  ).si_then([this, &t, FNAME](auto extent) {
	    TRACET("evict cold extent {}", t, *extent);
	    evict_state.cold_extents.emplace_back(extent);
	  });
	});
      }).si_then([this, &t, FNAME] {
	DEBUG("rewrite {}", evict_state.write_size);
	auto mtime = seastar::lowres_system_clock::now();
	return trans_intr::do_for_each(evict_state.cold_extents, [this, &t, mtime, FNAME](auto &extent) {
	  if (!extent->is_valid()) {
	    ERROR("{}", *extent);
	    ceph_abort();
	  }
	  extent->set_user_hint(placement_hint_t::EVICT);
	  return tm->rewrite_extent(t, extent, MIN_COLD_GENERATION, mtime);
	});
      }).si_then([this, &t] {
	return tm->submit_transaction_direct(t);
      }).si_then([this, FNAME] {
	stats.object_data_block_counts -= evict_state.cold_extents.size();
	stats.evicted_size += evict_state.write_size;
	stats.evicted_counts += evict_state.cold_extents.size();
	auto size = lru.size();
	for (auto l : evict_state.completed) {
	  remove_direct(l);
	}
	INFO("evict finished, {}", evict_state);
	if (size != lru.size() + evict_state.completed.size()) {
	  ERROR("size {} lru size {}", size, lru.size());
	}
	return seastar::now();
      });
    });
  }).handle_error(crimson::ct_error::assert_all{ "impossible" });
}

void OnodeCache::register_metrics() {
  namespace sm = seastar::metrics;
  metrics.add_group("onode_cache", {
      sm::make_counter("object_data_block_counts",
		       stats.object_data_block_counts,
		       sm::description("")),
      sm::make_counter("onode_counts",
		       [this] { return lru.size(); },
		       sm::description("")),
      sm::make_counter("added_size",
	               added_size,
		       sm::description("")),
      sm::make_counter("retired_size",
	               retired_size,
		       sm::description("")),
      sm::make_counter("cold_to_hot_write_size",
		       stats.read_size,
		       sm::description("")),
      sm::make_counter("in_memory_extents_size",
		       read_size,
		       sm::description("")),
      sm::make_counter("evicted_size",
		       stats.evicted_size,
		       sm::description("")),
  });
}

}
