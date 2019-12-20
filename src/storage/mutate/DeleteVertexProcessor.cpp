/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteVertexProcessor.h"
#include "base/NebulaKeyUtils.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void DeleteVertexProcessor::process(const cpp2::DeleteVertexRequest& req) {
    VLOG(3) << "Receive DeleteVertexRequest...";
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vid();
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId);
    std::vector<std::string> keys;
    keys.reserve(32);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId;
        this->onFinished();
        return;
    }
    while (iter->valid()) {
        auto key = iter->key();
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            if (NebulaKeyUtils::isVertex(key)) {
                auto tagId = NebulaKeyUtils::getTagId(key);
                VLOG(3) << "Evict vertex cache for vId " << vId << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vId, tagId), partId);
            }
        }
        keys.emplace_back(key.data(), key.size());
        iter->next();
    }

    this->kvstore_->asyncMultiRemove(spaceId,
                                     partId,
                                     std::move(keys),
                                     [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
        cpp2::ResultCode thriftResult;
        thriftResult.set_code(to(code));
        thriftResult.set_part_id(partId);
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(std::move(leader));
        }
        {
            std::lock_guard<std::mutex> lg(this->lock_);
            if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {
                this->codes_.emplace_back(std::move(thriftResult));
            }
        }
        this->onFinished();
    });
}

}  // namespace storage
}  // namespace nebula
