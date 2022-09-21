/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/StorageFlags.h"

DEFINE_string(store_type,
              "nebula",
              "Which type of KVStore to be used by the storage daemon."
              " Options can be \"nebula\", \"hbase\", etc.");

DEFINE_int32(waiting_catch_up_retry_times, 30, "retry times when waiting for catching up data");

DEFINE_int32(waiting_catch_up_interval_in_secs,
             30,
             "interval between two requests for catching up state");

DEFINE_int32(waiting_new_leader_retry_times, 5, "retry times when waiting for new leader");

DEFINE_int32(waiting_new_leader_interval_in_secs,
             5,
             "interval between two requests for catching up state");

DEFINE_uint32(rebuild_index_part_rate_limit,
              1024 * 512,
              "max bytes of rebuilding index for each partition in one second");

DEFINE_uint32(rebuild_index_batch_size, 1024 * 128, "batch size for rebuild index, in bytes");

DEFINE_int32(reader_handlers, 32, "Total reader handlers");

DEFINE_uint64(default_mvcc_ver,
              0L,
              "vertex/edge version if enable_multi_versions set to false."
              "this, has to be more than 0 if toss enabled. "
              "because we need lock before edge. ");

DEFINE_string(reader_handlers_type, "cpu", "Type of reader handlers, options: cpu,io");

DEFINE_bool(trace_toss, false, "output verbose log of toss");

DEFINE_int32(max_edge_returned_per_vertex, INT_MAX, "Max edge number returned searching vertex");

DEFINE_bool(query_concurrently,
            false,
            "whether to run query of each part concurrently, only lookup and "
            "go are supported");

DEFINE_bool(use_vertex_key, false, "whether allow insert or query the vertex key");
