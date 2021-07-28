/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGEFLAGS_H_
#define STORAGE_STORAGEFLAGS_H_

#include "common/base/Base.h"

DECLARE_string(store_type);

DECLARE_int32(waiting_catch_up_retry_times);

DECLARE_int32(waiting_catch_up_interval_in_secs);

DECLARE_int32(waiting_new_leader_retry_times);

DECLARE_int32(waiting_new_leader_interval_in_secs);

DECLARE_int32(rebuild_index_batch_num);

DECLARE_int32(rebuild_index_locked_threshold);

DECLARE_bool(enable_vertex_cache);

DECLARE_int32(reader_handlers);

DECLARE_uint64(default_mvcc_ver);

DECLARE_string(reader_handlers_type);

DECLARE_bool(trace_toss);

DECLARE_int32(max_edge_returned_per_vertex);

DECLARE_bool(query_concurrently);

#endif  // STORAGE_STORAGEFLAGS_H_
