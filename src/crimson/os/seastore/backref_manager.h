// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore::backref {

class BackrefManager {
public:
  using base_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using base_iertr = trans_iertr<base_ertr>;

  using mkfs_iertr = base_iertr;
  using mkfs_ret = mkfs_iertr::future<>;
  virtual mkfs_ret mkfs(
    Transaction &t) = 0;

  using get_mappings_iertr = base_iertr;
  using get_mappings_ret = get_mappings_iertr::future<backref_pin_list_t>;
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    paddr_t offset, extent_len_t length) = 0;

  using get_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using get_mapping_ret = get_mapping_iertr::future<BackrefPinRef>;
  virtual get_mapping_ret  get_mapping(
    Transaction &t,
    paddr_t offset) = 0;

  using rewrite_extent_iertr = base_iertr;
  using rewrite_extent_ret = rewrite_extent_iertr::future<>;
  virtual rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) = 0;

  using new_mapping_iertr = base_iertr;
  using new_mapping_ret = new_mapping_iertr::future<BackrefPinRef>;
  virtual new_mapping_ret new_mapping(
    Transaction &t,
    paddr_t key,
    extent_len_t len,
    laddr_t val,
    extent_types_t type) = 0;

  using batch_insert_iertr = base_iertr;
  using batch_insert_ret = batch_insert_iertr::future<>;
  virtual batch_insert_ret batch_insert(
    Transaction &t,
    backref_buffer_ref &bbr,
    const journal_seq_t &limit) = 0;

  virtual batch_insert_ret batch_insert_from_cache(
    Transaction &t,
    const journal_seq_t &limit) = 0;

  struct remove_mapping_result_t {
    paddr_t offset;
    extent_len_t len;
    laddr_t laddr;
  };

  using remove_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using remove_mapping_ret = remove_mapping_iertr::future<remove_mapping_result_t>;
  virtual remove_mapping_ret remove_mapping(
    Transaction &t,
    paddr_t offset) = 0;

  using scan_mapped_space_iertr = base_iertr::extend_ertr<
    SegmentManager::read_ertr>;
  using scan_mapped_space_ret = scan_mapped_space_iertr::future<>;
  using scan_mapped_space_func_t = std::function<
    void(paddr_t, extent_len_t, depth_t)>;
  virtual scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) = 0;

  virtual void add_pin(BackrefPin &pin) = 0;
  virtual void remove_pin(BackrefPin &pin) = 0;

  virtual ~BackrefManager() {}
};

using BackrefManagerRef =
  std::unique_ptr<BackrefManager>;

BackrefManagerRef create_backref_manager(
  SegmentManager &segment_manager,
  Cache &cache);

} // namespace crimson::os::seastore::backref
