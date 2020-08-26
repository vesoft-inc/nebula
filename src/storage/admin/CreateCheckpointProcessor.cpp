/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CreateCheckpointProcessor.h"

namespace nebula {
namespace storage {

void CreateCheckpointProcessor::process(const cpp2::CreateCPRequest& req) {
    CHECK_NOTNULL(kvstore_);
    LOG(INFO) << "Begin create checkpoint for space " << req.get_space_id();
    auto spaceId = req.get_space_id();
    auto& name = req.get_name();
    auto retCode = kvstore_->createCheckpoint(spaceId, std::move(name));
    if (retCode != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(to(retCode));
        codes_.emplace_back(std::move(thriftRet));
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula


