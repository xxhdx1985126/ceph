#pragma once

#include "include/lru.h"
#include "common/hobject.h"
#include "osd/osd_types.h"     
#include "include/buffer.h"
#include "include/encoding.h"
#include "common/debug.h" 


#include <cstddef>
#include <memory>
#include <unistd.h>
#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <optional>
#include <algorithm>
#include <set>

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
using ceph::encode;


struct object_info_cache {
  // ss
  snapid_t snap;
  // oi
  uint64_t size;
  // onode
  laddr_t object_data_laddr;
  laddr_t omap_root_laddr;
  laddr_t xattr_root_laddr;
  uint32_t extent_len;
  uint64_t version;


  object_info_cache() : snap(0), size(0) {}
  object_info_cache(snapid_t s, uint64_t sz) : snap(s), size(sz) {}

  object_info_cache& operator=(const object_info_cache& other) = default;
  bool operator==(const object_info_cache& other) const {
    return snap == other.snap && size == other.size;
  }
  bool operator!=(const object_info_cache& other) const {
    return !(*this == other);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(snap, bl);
    ceph::encode(size, bl);
    ceph::encode(object_data_laddr, bl);
    ceph::encode(omap_root_laddr, bl);
    ceph::encode(xattr_root_laddr, bl);
    ceph::encode(extent_len, bl);
    ceph::encode(version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    ceph::decode(snap, bl);
    ceph::decode(size, bl);
    ceph::decode(object_data_laddr, bl);
    ceph::decode(omap_root_laddr, bl);
    ceph::decode(xattr_root_laddr, bl);
    ceph::decode(extent_len, bl);
    ceph::decode(version, bl);
    DECODE_FINISH(bl);
  }
  // inline void encode(const object_info_cache& obj, ceph::buffer::list& bl) {
  //   obj.encode(bl);
  // }
  // inline void decode(object_info_cache& obj, ceph::buffer::list::const_iterator& bl) {
  //   obj.decode(bl);
  // }
};
WRITE_CLASS_ENCODER(object_info_cache);

class MetaData : public LRUObject {
  // key : object_t oid; value : MetaData
private:
  object_info_cache head_oi;
  SnapSet ss;
  std::map<snapid_t, object_info_cache> clones_metadata;
  object_t oid;
  // snap_set_context
  
  // int ref;
  // bool registered; // 是否在pg的ssc的缓存
public:
  MetaData(const MetaData&) = delete;
  MetaData& operator=(const MetaData&) = delete;
  
  MetaData(const object_info_cache& h_oi, const SnapSet& _ss, snapid_t target_snap_id, const object_info_cache& t_oi, const object_t& object_id);
  ~MetaData() = default;

  std::optional<object_info_cache> get_head_oi() const;
  std::optional<object_info_cache> get_clone_oi(snapid_t snap_id) const;
  const SnapSet& get_snapset() const;
  object_t get_oid() const { return oid; }
  // TODO set函数实现
  void set_snapset(const SnapSet& new_ss);
  void set_head_oi(const object_info_cache& new_head_oi);
  void set_clone_oi(snapid_t snap_id, const object_info_cache& clone_oi);
};

class MetaCacher {
private:
  class PGPartition {
  private:
    std::unique_ptr<LRU> metadata_lru;
    std::unordered_map<object_t, std::shared_ptr<MetaData>> metadata_map;
    std::size_t current_size;
    std::size_t max_size;
    mutable std::shared_mutex mutex_;

    object_t evict_one_from_lru();

  public:
    explicit PGPartition(std::size_t max_size);
    explicit PGPartition();
    ~PGPartition();
    
    int insert_or_update(const object_t& oid, std::shared_ptr<MetaData> metadata);
    std::shared_ptr<MetaData> get(const object_t& oid);
    int remove(const object_t& oid);
    std::size_t get_current_size() const { return current_size; }
  };

  std::unordered_map<pg_t, std::unique_ptr<PGPartition>> pg_map_lru;
  std::set<pg_t> pg_ids;
  mutable std::shared_mutex mutex_cache;

public:
  MetaCacher();
  ~MetaCacher() = default;
  
  const std::set<pg_t> get_pgids() const;
  std::shared_ptr<MetaData> get_metadata_from_cache(pg_t pgid, const object_t& oid);
  int add_or_update_metadata(pg_t pgid, const object_t& oid, std::shared_ptr<MetaData> metadata);
  int remove_metadata(pg_t pgid, const object_t& oid);
  int pgs_remove(const std::set<pg_t>& pgids);
};
