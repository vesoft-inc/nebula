/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/DropCheckpointProcessor.h"

namespace nebula {
namespace storage {

void DropCheckpointProcessor::process(const cpp2::DropCPRequest& req) {
    if (FLAGS_store_type != "nebula") {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }

    auto spaceId = req.get_space_id();
    auto& name = req.get_name();
    auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    auto retCode = store->dropCheckpoint(spaceId, std::move(name));
    if (retCode != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(to(retCode));
        codes_.emplace_back(std::move(thriftRet));
    }

    onFinished();
}

}  // namespace storage
}  // namespace nebula


