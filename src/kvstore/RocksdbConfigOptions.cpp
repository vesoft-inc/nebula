/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/RocksdbConfigOptions.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
              true,
              "rocksdb wal is close by default");

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

DEFINE_uint32(batch_reserved_bytes,
              4 * 1024,
              "default reserved bytes for one batch operation");

DEFINE_string(part_man_type,
              "memory",
              "memory, meta");
/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(block_cache, 4,
             "BlockBasedTable:block_cache : MB");

namespace nebula {
namespace kvstore {
rocksdb::Status RocksdbConfigOptions::initRocksdbOptions(rocksdb::Options &baseOpts) {
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

    bbtOpts.block_cache = rocksdb::NewLRUCache(FLAGS_block_cache * 1024 * 1024);
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.create_if_missing = true;
    return s;
}
}  // namespace kvstore
}  // namespace nebula
