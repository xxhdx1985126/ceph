// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/object_context.h"

#include <fmt/ranges.h>

#include "common/Formatter.h"
#include "crimson/common/config_proxy.h"

using namespace std::string_literals;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

ObjectContextRegistry::ObjectContextRegistry(crimson::common::ConfigProxy &conf)
{
  obc_lru.set_target_size(conf.get_val<uint64_t>("crimson_osd_obc_lru_size"));
  conf.add_observer(this);
}

ObjectContextRegistry::~ObjectContextRegistry()
{
  // purge the cache to avoid leaks and complains from LSan
  obc_lru.set_target_size(0UL);
}

std::vector<std::string> ObjectContextRegistry::get_tracked_keys() const noexcept
{
  return {"crimson_osd_obc_lru_size"s};
}

void ObjectContextRegistry::handle_conf_change(
  const crimson::common::ConfigProxy& conf,
  const std::set <std::string> &changed)
{
  obc_lru.set_target_size(conf.get_val<uint64_t>("crimson_osd_obc_lru_size"));
}

std::optional<hobject_t> resolve_oid(
  const SnapSet &ss,
  const hobject_t &oid)
{
  logger().debug("{} oid.snap={},head snapset.seq={}",
                 __func__, oid.snap, ss.seq);
  
  // ​​快照系统中对象版本定位
  // 请求的快照大于最新快照
  if (oid.snap > ss.seq) {
    // Because oid.snap > ss.seq, we are trying to read from a snapshot
    // taken after the most recent write to this object. Read from head.
    logger().debug("{} returning head", __func__);
    return oid.get_head();
  } else {
    // which clone would it be?
    // 在有序克隆列表(ss.clones)中二分查找
    auto clone = std::lower_bound(
      begin(ss.clones), end(ss.clones),
      oid.snap);
    if (clone == end(ss.clones)) {
      // Doesn't exist, > last clone, < ss.seq
      return std::nullopt;
    }
    auto citer = ss.clone_snaps.find(*clone);

    // 克隆快照映射示例
    // ss.clone_snaps = {
    //   {10, {5, 8}},  // 克隆10包含快照5和8
    //   {20, {15}}     // 克隆20包含快照15
    // }
    // key：数据和快照信息的集合，是clone（存储）
    // value：用户创建的快照点，是快照（时间）
    // TODO: how do we want to handle this kind of logic error?
    ceph_assert(citer != ss.clone_snaps.end());

    if (std::find(
      citer->second.begin(),
      citer->second.end(),
      oid.snap) == citer->second.end()) {
       logger().debug("{} {} does not contain {} -- DNE",
                      __func__, ss.clone_snaps, oid.snap);
       return std::nullopt;
    } else {
      auto soid = oid;
      soid.snap = *clone;
      return std::optional<hobject_t>(soid);
    }
  }
}

}
