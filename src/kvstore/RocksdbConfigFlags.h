/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H
#define NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H

#include "base/Base.h"

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

// BlockBasedTable block_cache
DECLARE_int64(block_cache);

DECLARE_uint32(batch_reserved_bytes);

DECLARE_string(part_man_type);

#endif  // NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H
