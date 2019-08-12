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

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
            false,
            "Whether to disable the WAL in rocksdb");

// [DBOptions]
DEFINE_string(rocksdb_db_options,
              "",
              "DBOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "",
              "ColumnFamilyOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "",
              "BlockBasedTableOptions, each option will be given "
              "as <option_name>:<option_value> separated by ;");

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
    s = GetDBOptionsFromString(rocksdb::DBOptions(),
            FLAGS_rocksdb_db_options, &dbOpts);
    if (!s.ok()) {
        return s;
    }

    s = GetColumnFamilyOptionsFromString(rocksdb::ColumnFamilyOptions(),
            FLAGS_rocksdb_column_family_options, &cfOpts);

    if (!s.ok()) {
        return s;
    }

    baseOpts = rocksdb::Options(dbOpts, cfOpts);

    s = GetBlockBasedTableOptionsFromString(rocksdb::BlockBasedTableOptions(),
            FLAGS_rocksdb_block_based_table_options, &bbtOpts);

    if (!s.ok()) {
        return s;
    }

    bbtOpts.block_cache = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024);
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
    return s;
}

}  // namespace kvstore
}  // namespace nebula
