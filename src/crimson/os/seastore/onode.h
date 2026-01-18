// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/hobject.h"
#include "include/byteorder.h"
#include "seastore_types.h"

namespace crimson::os::seastore {

struct onode_layout_t {
  // The expected decode size of object_info_t without oid.
  // 在测试的时候需要把这两个值调为1，这两个值是用来控制oi和ss是存在onode还是omap的
  // 我们希望有缓存之后，不查onode，所以希望oi和ss都存在omap里面，方便后面的修改
  static constexpr int MAX_OI_LENGTH = 1;
  // We might want to move the ss field out of onode_layout_t.
  // The reason is that ss_attr may grow to relative large, as
  // its clone_overlap may grow to a large size, if applications
  // set objects to a relative large size(for the purpose of reducing
  // the number of objects per OSD, so that all objects' metadata
  // can be cached in memory) and do many modifications between
  // snapshots.
  // TODO: implement flexible-sized onode value to store inline ss_attr
  // effectively.
  // The expected decode size of SnapSet when there's no snapshot
  static constexpr int MAX_SS_LENGTH = 1;

  ceph_le32 size{0};
  ceph_le32 oi_size{0};
  ceph_le32 ss_size{0};
  omap_root_le_t omap_root;
  omap_root_le_t log_root;
  omap_root_le_t xattr_root;

  object_data_le_t object_data;

  char oi[MAX_OI_LENGTH] = {0};
  char ss[MAX_SS_LENGTH] = {0};

  onode_layout_t() : omap_root(omap_type_t::OMAP), log_root(omap_type_t::LOG),
    xattr_root(omap_type_t::XATTR) {}

  const omap_root_le_t& get_root(omap_type_t type) const {
    if (type == omap_type_t::XATTR) {
      return xattr_root;
    } else if (type == omap_type_t::OMAP) {
      return omap_root;
    } else {
      assert(type == omap_type_t::LOG);
      return log_root;
    }
  }
} __attribute__((packed));

class Transaction;

/**
 * Onode
 *
 * Interface manipulated by seastore.  OnodeManager implementations should
 * return objects derived from this interface with layout referencing
 * internal representation of onode_layout_t.
 */
class Onode : public boost::intrusive_ref_counter<
  Onode,
  boost::thread_unsafe_counter>
{
protected:
  virtual laddr_t get_hint() const = 0;
  const uint32_t default_metadata_offset = 0;
  const uint32_t default_metadata_range = 0;
  const hobject_t hobj;
public:
  Onode() = default;
  Onode(uint32_t ddr, uint32_t dmr, const hobject_t &hobj)
    : default_metadata_offset(ddr),
      default_metadata_range(dmr),
      hobj(hobj)
  {}

  virtual const onode_layout_t &get_layout() const = 0;
  virtual ~Onode() = default;

  virtual void update_onode_size(Transaction&, uint32_t) = 0;
  virtual void update_omap_root(Transaction&, omap_root_t&) = 0;
  virtual void update_log_root(Transaction&, omap_root_t&) = 0;
  virtual void update_xattr_root(Transaction&, omap_root_t&) = 0;
  virtual void update_object_data(Transaction&, object_data_t&) = 0;
  virtual void update_object_info(Transaction&, ceph::bufferlist&) = 0;
  virtual void update_snapset(Transaction&, ceph::bufferlist&) = 0;
  virtual void clear_object_info(Transaction&) = 0;
  virtual void clear_snapset(Transaction&) = 0;

  laddr_t get_metadata_hint(uint64_t block_size) const {
    assert(default_metadata_offset);
    assert(default_metadata_range);
    uint64_t range_blocks = default_metadata_range / block_size;
    auto random_offset = default_metadata_offset +
        (((uint32_t)std::rand() % range_blocks) * block_size);
    return (get_hint() + random_offset).checked_to_laddr();
  }
  laddr_t get_data_hint() const {
    return get_hint();
  }
  const omap_root_le_t& get_root(omap_type_t type) const {
    return get_layout().get_root(type);
  }
  virtual const hobject_t &get_hobj() const {
    return hobj;
  }
  virtual laddr_t get_object_data_base() const {
    return get_layout().object_data.get().get_reserved_data_base();
  }
  virtual laddr_t get_omap_root_base() const {
    return get_root(omap_type_t::OMAP).get_laddr();
  }
  virtual laddr_t get_xattr_root_base() const {
    return get_root(omap_type_t::XATTR).get_laddr();
  }
  virtual uint32_t get_size() const {
    return get_layout().size;
  }
  virtual onode_info_cache get_onode_info_cache() const {
    return onode_info_cache(
      get_hobj(),
      get_object_data_base(),
      get_omap_root_base(),
      get_xattr_root_base(),
      get_size(),
      0);
  }
  friend std::ostream& operator<<(std::ostream &out, const Onode &rhs);
};

class CachedOnode : public Onode {
public:
  CachedOnode() = default;
  const onode_layout_t &get_layout() const final {
    ceph_abort("impossible");
  }
  void update_onode_size(Transaction&, uint32_t) final {
    ceph_abort("impossible");
  }
  void update_omap_root(Transaction&, omap_root_t&) final {
    ceph_abort("impossible");
  }
  void update_log_root(Transaction&, omap_root_t&) final {
    ceph_abort("impossible");
  }
  void update_xattr_root(Transaction&, omap_root_t&) final {
    ceph_abort("impossible");
  }
  void update_object_data(Transaction&, object_data_t&) final {
    ceph_abort("impossible");
  }
  void update_object_info(Transaction&, ceph::bufferlist&) final {
    ceph_abort("impossible");
  }
  void update_snapset(Transaction&, ceph::bufferlist&) final {
    ceph_abort("impossible");
  }
  void clear_object_info(Transaction&) final {
    ceph_abort("impossible");
  }
  void clear_snapset(Transaction&) final {
    ceph_abort("impossible");
  }

  const hobject_t &get_hobj() const final {
    return cached_onode_info->oid;
  }
  void set_onode_info(onode_info_cache &cached_onode_info) {
    this->cached_onode_info = cached_onode_info;
  }
  bool has_value() const {
    return cached_onode_info.has_value();
  }
  laddr_t get_object_data_base() const final {
    return cached_onode_info->object_data_laddr;
  }
  laddr_t get_omap_root_base() const final {
    return cached_onode_info->omap_root_laddr;
  }
  laddr_t get_xattr_root_base() const final {
    return cached_onode_info->xattr_root_laddr;
  }
  uint32_t get_size() const final {
    return cached_onode_info->size;
  }
  onode_info_cache get_onode_info_cache() const final {
    return std::move(*cached_onode_info);
  }
private:
  laddr_t get_hint() const { ceph_abort("impossible"); }
  std::optional<onode_info_cache> cached_onode_info;
};

std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::Onode> : fmt::ostream_formatter {};
#endif
