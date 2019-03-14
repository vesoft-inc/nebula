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
DECLARE_string(stats_dump_period_sec);

// [CFOptions "default"]

//  [TableOptions/BlockBasedTable "default"]

// memtable_factory
DECLARE_string(memtable_factory);

// rocksdb db wal disable
DECLARE_bool(rocksdb_disable_wal);

#endif  // NEBULA_GRAPH_ROCKSDBCONFIGFLAGS_H
