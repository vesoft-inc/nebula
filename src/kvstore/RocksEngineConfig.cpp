/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/RocksEngineConfig.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "base/Configuration.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
            false,
            "Whether to disable the WAL in rocksdb");

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

DEFINE_string(part_man_type,
              "memory",
              "memory, meta");
/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(rocksdb_block_cache, 4,
             "The default block cache size used in BlockBasedTable. The unit is MB");


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

    bbtOpts.block_cache = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024);
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
        map.emplace(key, val.asString());
    });
    return true;
}

}  // namespace kvstore
}  // namespace nebula
