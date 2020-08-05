/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
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
DEFINE_bool(enable_multi_versions, false, "If true, the insert timestamp will be the wall clock. "
                                          "If false, always has the same timestamp of max");
