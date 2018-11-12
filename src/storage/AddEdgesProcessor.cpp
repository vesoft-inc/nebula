/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/AddEdgesProcessor.h"
#include <algorithm>
#include "time/TimeUtils.h"
#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    auto now = startMs_ = time::TimeUtils::nowInMSeconds();
    callingNum_ = req.edges.size();
    CHECK_NOTNULL(kvstore_);
    std::for_each(req.edges.begin(), req.edges.end(), [&](auto& partEdges){
        auto partId = partEdges.first;
        std::vector<kvstore::KV> data;
        std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge){
            auto key = KeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                         edge.key.dst, edge.key.ranking, now);
            data.emplace_back(std::move(key), std::move(edge.get_props()));
        });
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
