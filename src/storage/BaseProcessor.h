/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_BASEPROCESSOR_H_
#define STORAGE_BASEPROCESSOR_H_

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/stats/StatsManager.h"
#include "common/stats/Stats.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "storage/CommonUtils.h"
#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {

using PartCode = std::pair<PartitionID, kvstore::ResultCode>;

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(StorageEnv* env,
                           stats::Stats* stats = nullptr)
            : env_(env)
            , stats_(stats) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    void onFinished() {
        stats::Stats::addStatsValue(stats_,
                                    this->result_.get_failed_parts().empty(),
                                    this->duration_.elapsedInUSec());
        this->result_.set_latency_in_us(this->duration_.elapsedInUSec());
        this->result_.set_failed_parts(this->codes_);
        this->resp_.set_result(std::move(this->result_));
        this->promise_.setValue(std::move(this->resp_));
        delete this;
    }

    cpp2::ErrorCode getSpaceVidLen(GraphSpaceID spaceId) {
        auto len = this->env_->schemaMan_->getSpaceVidLen(spaceId);
        if (!len.ok()) {
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        spaceVidLen_ = len.value();
        return cpp2::ErrorCode::SUCCEEDED;
    }

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    kvstore::ResultCode doSyncPut(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  std::vector<kvstore::KV> data);

    void doRemove(GraphSpaceID spaceId, PartitionID partId, std::vector<std::string> keys);

    cpp2::ErrorCode to(kvstore::ResultCode code);

    cpp2::ErrorCode writeResultTo(WriteResult code, bool isEdge);

    nebula::meta::cpp2::ColumnDef columnDef(std::string name,
                                            nebula::meta::cpp2::PropertyType type);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId, HostAddr leader);

    void handleErrorCode(kvstore::ResultCode code, GraphSpaceID spaceId, PartitionID partId);

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId);

    void handleAsync(GraphSpaceID spaceId,
                     PartitionID partId,
                     kvstore::ResultCode code,
                     bool processFlyingRequest = true);

    StatusOr<std::string> encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                       const std::vector<std::string>& propNames,
                                       const std::vector<Value>& props,
                                       WriteResult& wRet);

protected:
    StorageEnv*                                     env_{nullptr};
    stats::Stats*                                   stats_{nullptr};
    RESP                                            resp_;
    folly::Promise<RESP>                            promise_;
    cpp2::ResponseCommon                            result_;

    std::unique_ptr<PlanContext>                    planContext_;
    time::Duration                                  duration_;
    std::vector<cpp2::PartitionResult>              codes_;
    std::mutex                                      lock_;
    int32_t                                         callingNum_{0};
    int32_t                                         spaceVidLen_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
