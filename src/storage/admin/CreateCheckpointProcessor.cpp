/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CreateCheckpointProcessor.h"

namespace nebula {
namespace storage {

void CreateCheckpointProcessor::process(const cpp2::CreateCPRequest& req) {
    CHECK_NOTNULL(env_);
    auto spaceId = req.get_space_id();
    auto& name = req.get_name();
    auto ret = env_->kvstore_->createCheckpoint(spaceId, std::move(name));
    if (!ok(ret)) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(to(error(ret)));
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }

    auto& info = nebula::value(ret);
    resp_.set_path(std::move(info.first));
    resp_.set_partition_info(std::move(info.second));
    onFinished();
}

}  // namespace storage
}  // namespace nebula


