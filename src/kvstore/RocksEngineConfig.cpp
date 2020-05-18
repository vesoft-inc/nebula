/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/RocksEngineConfig.h"
#include "kvstore/EventListner.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/filter_policy.h"
#include "base/Configuration.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
            true,
            "Whether to disable the WAL in rocksdb");

DEFINE_bool(rocksdb_wal_sync,
            false,
            "Whether WAL writes are synchronized to disk or not");

// [DBOptions]
DEFINE_string(rocksdb_db_options,
              "{}",
              "json string of DBOptions, all keys and values are string");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "{}",
              "json string of ColumnFamilyOptions, all keys and values are string");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "{}",
              "json string of BlockBasedTableOptions, all keys and values are string");

DEFINE_int32(rocksdb_batch_size,
             4 * 1024,
             "default reserved bytes for one batch operation");
/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(rocksdb_block_cache, 1024,
             "The default block cache size used in BlockBasedTable. The unit is MB");

DEFINE_bool(enable_partitioned_index_filter, false, "True for partitioned index filters");

namespace nebula {
namespace kvstore {

rocksdb::Status initRocksdbOptions(rocksdb::Options &baseOpts) {
    rocksdb::Status s;
    rocksdb::DBOptions dbOpts;
    rocksdb::ColumnFamilyOptions cfOpts;
    rocksdb::BlockBasedTableOptions bbtOpts;

    std::unordered_map<std::string, std::string> dbOptsMap;
    if (!loadOptionsMap(dbOptsMap, FLAGS_rocksdb_db_options)) {
        return rocksdb::Status::InvalidArgument();
    }
    s = GetDBOptionsFromMap(rocksdb::DBOptions(), dbOptsMap, &dbOpts, true);
    if (!s.ok()) {
        return s;
    }
    dbOpts.listeners.emplace_back(new EventListener());

    std::unordered_map<std::string, std::string> cfOptsMap;
    if (!loadOptionsMap(cfOptsMap, FLAGS_rocksdb_column_family_options)) {
        return rocksdb::Status::InvalidArgument();
    }
    s = GetColumnFamilyOptionsFromMap(rocksdb::ColumnFamilyOptions(), cfOptsMap, &cfOpts, true);
    if (!s.ok()) {
        return s;
    }

    baseOpts = rocksdb::Options(dbOpts, cfOpts);

    std::unordered_map<std::string, std::string> bbtOptsMap;
    if (!loadOptionsMap(bbtOptsMap, FLAGS_rocksdb_block_based_table_options)) {
        return rocksdb::Status::InvalidArgument();
    }
    s = GetBlockBasedTableOptionsFromMap(rocksdb::BlockBasedTableOptions(), bbtOptsMap,
                                         &bbtOpts, true);
    if (!s.ok()) {
        return s;
    }

    static std::shared_ptr<rocksdb::Cache> blockCache
        = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024);
    bbtOpts.block_cache = blockCache;
    bbtOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    if (FLAGS_enable_partitioned_index_filter) {
        bbtOpts.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
        bbtOpts.partition_filters = true;
        bbtOpts.cache_index_and_filter_blocks = true;
        bbtOpts.cache_index_and_filter_blocks_with_high_priority = true;
        bbtOpts.pin_l0_filter_and_index_blocks_in_cache =
            baseOpts.compaction_style == rocksdb::CompactionStyle::kCompactionStyleLevel;
    }
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
    return s;
}

bool loadOptionsMap(std::unordered_map<std::string, std::string> &map, const std::string& gflags) {
    Configuration conf;
    auto status = conf.parseFromString(gflags);
    if (!status.ok()) {
        return false;
    }
    conf.forEachItem([&map] (const std::string& key, const folly::dynamic& val) {
        LOG(INFO) << "Emplace rocksdb option " << key << "=" << val.asString();
        map.emplace(key, val.asString());
    });
    return true;
}

}  // namespace kvstore
}  // namespace nebula
