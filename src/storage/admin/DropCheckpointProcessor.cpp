/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/DropCheckpointProcessor.h"

namespace nebula {
namespace storage {

void DropCheckpointProcessor::process(const cpp2::DropCPRequest& req) {
    CHECK_NOTNULL(kvstore_);
    LOG(INFO) << "Begin drop checkpoint for space " << req.get_space_id();
    auto spaceId = req.get_space_id();
    auto& name = req.get_name();
    auto retCode = kvstore_->dropCheckpoint(spaceId, std::move(name));
    if (retCode != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(to(retCode));
        codes_.emplace_back(std::move(thriftRet));
    }

    onFinished();
}

}  // namespace storage
}  // namespace nebula


