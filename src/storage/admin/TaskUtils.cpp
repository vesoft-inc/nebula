/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/TaskUtils.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode toStorageErr(kvstore::ResultCode code) {
    switch (code) {
    case kvstore::ResultCode::SUCCEEDED:
        return cpp2::ErrorCode::SUCCEEDED;
    case kvstore::ResultCode::ERR_LEADER_CHANGED:
        return cpp2::ErrorCode::E_LEADER_CHANGED;
    case kvstore::ResultCode::ERR_SPACE_NOT_FOUND:
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    case kvstore::ResultCode::ERR_PART_NOT_FOUND:
        return cpp2::ErrorCode::E_PART_NOT_FOUND;
    case kvstore::ResultCode::ERR_CONSENSUS_ERROR:
        return cpp2::ErrorCode::E_CONSENSUS_ERROR;
    case kvstore::ResultCode::ERR_CHECKPOINT_ERROR:
        return cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    case kvstore::ResultCode::ERR_WRITE_BLOCK_ERROR:
        return cpp2::ErrorCode::E_CHECKPOINT_BLOCKED;
    case kvstore::ResultCode::ERR_PARTIAL_RESULT:
        return cpp2::ErrorCode::E_PARTIAL_RESULT;
    default:
        return cpp2::ErrorCode::E_UNKNOWN;
    }
}

}  // namespace storage
}  // namespace nebula
