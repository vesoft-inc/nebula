/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/clients/storage/StorageClientBase.h"

DEFINE_int32(storage_client_timeout_ms, 60 * 1000, "storage client timeout");
DEFINE_uint32(storage_client_retry_interval_ms, 1000,
             "storage client sleep interval milliseconds between retry");

namespace nebula {
namespace storage {
}   // namespace storage
}   // namespace nebula
