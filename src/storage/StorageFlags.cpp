/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"

DEFINE_string(store_type, "nebula",
              "Which type of KVStore to be used by the storage daemon."
              " Options can be \"nebula\", \"hbase\", etc.");

DEFINE_int32(waiting_catch_up_retry_times, 30, "retry times when waiting for catching up data");

DEFINE_int32(waiting_catch_up_interval_in_secs, 30,
             "interval between two requests for catching up state");

DEFINE_int32(waiting_new_leader_retry_times, 30, "retry times when waiting for catching up data");

DEFINE_int32(waiting_new_leader_interval_in_secs, 5,
             "interval between two requests for catching up state");

DEFINE_int32(rebuild_index_batch_num, 1024,
             "The batch size when rebuild index");

DEFINE_int32(rebuild_index_locked_threshold, 1024,
             "The locked threshold will refuse writing.");

DEFINE_int32(vertex_cache_num, 16 * 1000 * 1000, "Total keys inside the cache");

DEFINE_int32(vertex_cache_bucket_exp, 4, "Total buckets number is 1 << cache_bucket_exp");

DEFINE_bool(enable_vertex_cache, true, "Enable vertex cache");

DEFINE_int32(reader_handlers, 32, "Total reader handlers");

DEFINE_bool(enable_multi_versions, false, "If true, the insert timestamp will be the wall clock. "
                                          "If false, always has the same timestamp of max");

DEFINE_uint64(default_mvcc_ver, 0L, "vertex/edge version if enable_multi_versions set to false."
                               "this, has to be more than 0 if toss enabled. "
                               "because we need lock before edge. ");


DEFINE_string(reader_handlers_type, "cpu", "Type of reader handlers, options: cpu,io");

DEFINE_bool(trace_toss, false, "output verbose log of toss");
