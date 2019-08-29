/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/DeleteVertexProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteVertexProcessor::process(const cpp2::DeleteVertexRequest& req) {
    VLOG(3) << "Receive DeleteVertexRequest...";

    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vid();
    callingNum_ = 1;
    CHECK_NOTNULL(kvstore_);

    std::vector<std::string> deleteKeys;
    auto prefix = NebulaKeyUtils::prefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId = " << spaceId
                << ", partId =  " << partId << ", vertexId = " << vId;
        this->pushResultCode(this->to(ret), partId);
        this->onFinished();
        return;
    }
    while (iter->valid()) {
        auto key = iter->key();
        if (NebulaKeyUtils::isVertex(key)) {
            deleteKeys.emplace_back(key);
        }
        iter->next();
    }

    doRemove(spaceId, partId, deleteKeys);
}

}  // namespace storage
}  // namespace nebula
