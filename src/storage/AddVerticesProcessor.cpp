/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/AddVerticesProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "time/TimeUtils.h"
#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto now = startMs_ = time::TimeUtils::nowInMSeconds();
    const auto& partVertices = req.get_vertices();
    auto spaceId = req.get_space_id();
    callingNum_ = partVertices.size();
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto partId = pv.first;
        const auto& vertices = pv.second;
        std::vector<kvstore::KV> data;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v){
            const auto& props = v.get_props();
            std::for_each(props.begin(), props.end(), [&](auto& prop) {
                auto key = KeyUtils::vertexKey(partId, v.get_id(),
                                               prop.get_tag_id(), now);
                auto val = std::move(prop.get_props());
                data.emplace_back(std::move(key), std::move(val));
            });
        });
        CHECK_NOTNULL(kvstore_);
        kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                [partId, this](kvstore::ResultCode code, HostAddr addr) {
            cpp2::ResultCode thriftResult;
            thriftResult.code = to(code);
            thriftResult.part_id = partId;
            if (code == kvstore::ResultCode::ERR_LEADER_CHANAGED) {
                thriftResult.get_leader()->ip = addr.first;
                thriftResult.get_leader()->port = addr.second;
            }
            std::lock_guard<folly::SpinLock> lg(this->lock_);
            this->codes_.emplace_back(std::move(thriftResult));
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                this->resp_.set_codes(std::move(this->codes_));
                this->resp_.set_latency_in_ms(time::TimeUtils::nowInMSeconds() - this->startMs_);
                this->onFinished();
            }
        });
     });
}

}  // namespace storage
}  // namespace nebula
