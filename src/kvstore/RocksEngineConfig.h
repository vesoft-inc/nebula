/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_ROCKSENGINECONFIG_H_
#define KVSTORE_ROCKSENGINECONFIG_H_

#include <rocksdb/db.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

// [Version]
DECLARE_string(rocksdb_options_version);

// [DBOptions]
DECLARE_string(rocksdb_db_options);

// [CFOptions "default"]
DECLARE_string(rocksdb_column_family_options);

//  [TableOptions/BlockBasedTable "default"]
DECLARE_string(rocksdb_block_based_table_options);

// memtable_factory
DECLARE_string(memtable_factory);

// rocksdb db wal disable
DECLARE_bool(rocksdb_disable_wal);

DECLARE_bool(rocksdb_wal_sync);

// BlockBasedTable block_cache
DECLARE_int64(rocksdb_block_cache);

DECLARE_int32(rocksdb_batch_size);

DECLARE_int32(rocksdb_row_cache_num);

DECLARE_int32(cache_bucket_exp);

// rocksdb table format
DECLARE_string(rocksdb_table_format);

DECLARE_string(part_man_type);

DECLARE_string(rocksdb_compression_per_level);
DECLARE_string(rocksdb_compression);
DECLARE_string(rocksdb_bottommost_compression);

DECLARE_bool(enable_rocksdb_statistics);
DECLARE_string(rocksdb_stats_level);

DECLARE_bool(enable_rocksdb_prefix_filtering);
DECLARE_bool(enable_rocksdb_whole_key_filtering);

// rocksdb compact RangeOptions
DECLARE_bool(rocksdb_compact_change_level);
DECLARE_int32(rocksdb_compact_target_level);

DECLARE_string(rocksdb_wal_dir);
DECLARE_string(rocksdb_backup_dir);
DECLARE_int32(rocksdb_backup_interval_secs);

// rocksdb key value separation options
DECLARE_bool(rocksdb_enable_kv_separation);
DECLARE_uint64(rocksdb_kv_separation_threshold);

namespace nebula {
namespace kvstore {

/**
 * @brief Build rocksdb options form gflags
 *
 * @param baseOpts Rocksdb options
 * @param spaceId
 * @param vidLen
 * @return rocksdb::Status
 */
rocksdb::Status initRocksdbOptions(rocksdb::Options &baseOpts,
                                   GraphSpaceID spaceId,
                                   int32_t vidLen = 8);

/**
 * @brief Load a gflag into map
 *
 * @param map
 * @param gflags
 * @return Return succeeded or not
 */
bool loadOptionsMap(std::unordered_map<std::string, std::string> &map, const std::string &gflags);

/**
 * @brief Retrieve rocksdb statistics, return nullptr if not enabled
 */
std::shared_ptr<rocksdb::Statistics> getDBStatistics();

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_ROCKSENGINECONFIG_H_
