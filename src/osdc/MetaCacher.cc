
#ifdef WITH_CRIMSON
#include "MetaCacher.h"

#include "osd/OSDMap.h"

using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::decode_nohead;
using ceph::encode;
using ceph::encode_nohead;
using ceph::Formatter;
using ceph::make_timespan;
using ceph::JSONFormatter;

MetaData::MetaData(const object_info_cache& h_oi, const SnapSet& _ss, snapid_t target_snap_id, const object_info_cache& t_oi, const object_t& object_id)
    : head_oi(h_oi), ss(_ss), oid(object_id) {
  clones_metadata.insert(std::make_pair(target_snap_id, t_oi));
}

std::optional<object_info_cache> MetaData::get_head_oi() const {
  if (head_oi.size != static_cast<uint64_t>(-1)) { // 假设-1表示无效值
    return head_oi;
  }
  return std::nullopt;
}

std::optional<object_info_cache> MetaData::get_clone_oi(snapid_t snap_id) const {
  if (snap_id > ss.seq) {
    return head_oi;  // 返回头对象的对象信息
  } else {
    // 在克隆列表中找到第一个 >= snap_id 的克隆
    auto clone = std::lower_bound(
      begin(ss.clones), end(ss.clones),
      snap_id);
      
    if (clone == end(ss.clones)) {
      return std::nullopt;  // 没有找到合适的克隆
    }
      
    // 检查找到的克隆是否包含我们需要的快照
    auto citer = ss.clone_snaps.find(*clone);
    if (citer == ss.clone_snaps.end()) {
      return std::nullopt;
    }
    
    // 检查该克隆的快照列表是否包含目标快照
    if (std::find(citer->second.begin(), citer->second.end(), snap_id) 
        == citer->second.end()) {
      return std::nullopt;
    } else {
      // 找到对应的克隆对象信息
      auto target_clone = *clone;
      auto it = clones_metadata.find(target_clone);
      if (it != clones_metadata.end()) {
          return it->second;
      } else {
          return std::nullopt;
      }
    }
  }
}

const SnapSet& MetaData::get_snapset() const {
  return ss;
}

// PGPartition 实现
MetaCacher::PGPartition::PGPartition(std::size_t max_size) 
    : current_size(0), max_size(max_size) {
  metadata_lru = std::make_unique<LRU>();
}

MetaCacher::PGPartition::PGPartition() 
    : current_size(0) {
  metadata_lru = std::make_unique<LRU>();
    max_size=10000000;

}

MetaCacher::PGPartition::~PGPartition() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  metadata_map.clear();
  current_size = 0;
}

int MetaCacher::PGPartition::insert_or_update(const object_t& oid, std::shared_ptr<MetaData> metadata) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  auto it = metadata_map.find(oid);
  if (it != metadata_map.end()) {
    it->second = metadata;
    metadata_lru->lru_touch(it->second.get());
    return 0;
  }

  // 检查空间并淘汰
  if (current_size >= max_size) {
    ceph_assert(current_size <= max_size);
    object_t key = evict_one_from_lru();
    if(key == object_t()) return -1;  
    metadata_map.erase(key);
    current_size--;
  }

  // 插入新项
  metadata_map[oid] = metadata;
  current_size++;
  metadata_lru->lru_insert_top(metadata.get());
  
  return 0;
}

std::shared_ptr<MetaData> MetaCacher::PGPartition::get(const object_t& oid) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = metadata_map.find(oid);
  if (it != metadata_map.end()) {
    metadata_lru->lru_touch(it->second.get());
    return it->second;
  }
  return nullptr;
}

int MetaCacher::PGPartition::remove(const object_t& oid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = metadata_map.find(oid);
  if (it == metadata_map.end()) {
    return -1;
  }
  
  metadata_lru->lru_remove(it->second.get());
  metadata_map.erase(it);
  if (current_size > 0) {
    current_size--; 
  }
  return 0;
}

object_t MetaCacher::PGPartition::evict_one_from_lru() {
  LRUObject* expired = metadata_lru->lru_expire();
  if (!expired) {
    return object_t();  // 返回默认构造的对象
  }
  
  MetaData* meta = static_cast<MetaData*>(expired);
  return meta->get_oid();
}

// MetaCacher 实现
MetaCacher::MetaCacher() {
  pg_map_lru.max_load_factor(2.0f);
}

const std::set<pg_t> MetaCacher::get_pgids() const {
  std::shared_lock<std::shared_mutex> lock(mutex_cache);
  return pg_ids;
}

std::shared_ptr<MetaData> MetaCacher::get_metadata_from_cache(pg_t pgid, const object_t& oid) {
  std::shared_lock<std::shared_mutex> lock(mutex_cache);
  auto it = pg_map_lru.find(pgid);
  if (it != pg_map_lru.end()) {
    return it->second->get(oid);
  }
  return nullptr;
}

int MetaCacher::add_or_update_metadata(pg_t pgid, const object_t& oid, std::shared_ptr<MetaData> metadata) {
  std::unique_lock<std::shared_mutex> lock(mutex_cache);
  
  auto it = pg_map_lru.find(pgid);
  if (it == pg_map_lru.end()) {
    try {
      auto partition = std::make_unique<PGPartition>(10000000);
      auto [new_it, inserted] = pg_map_lru.insert({pgid, std::move(partition)});
      it = new_it;
      pg_ids.insert(pgid);
    } catch (const std::exception& e) {
      return -ENOMEM;
    }
  }
  ceph_assert(it->second);
  
  return it->second->insert_or_update(oid, metadata);
}

int MetaCacher::remove_metadata(pg_t pgid, const object_t& oid) {
  std::unique_lock<std::shared_mutex> lock(mutex_cache);
  auto it = pg_map_lru.find(pgid);
  if (it != pg_map_lru.end()) {
      return it->second->remove(oid);
  }
  return -1;
}

int MetaCacher::pgs_remove(const std::set<pg_t>& pgids) {
  std::unique_lock<std::shared_mutex> lock(mutex_cache);
  int count = 0;
  for (const auto& pgid : pgids) {
    auto it = pg_map_lru.find(pgid);
    if (it != pg_map_lru.end()) {
      pg_map_lru.erase(it);
      pg_ids.erase(pgid);
      count++;
    }
  }
  return count;
}
#endif
