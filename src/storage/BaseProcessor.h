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
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include "storage/CommonUtils.h"
#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {


template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(StorageEnv* env, const ProcessorCounters* counters = nullptr)
            : env_(env)
            , counters_(counters) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    virtual void onFinished() {
        if (counters_) {
            stats::StatsManager::addValue(counters_->numCalls_);
            if (!this->result_.get_failed_parts().empty()) {
                stats::StatsManager::addValue(counters_->numErrors_);
            }
        }

        this->result_.set_latency_in_us(this->duration_.elapsedInUSec());
        this->result_.set_failed_parts(this->codes_);
        this->resp_.set_result(std::move(this->result_));
        this->promise_.setValue(std::move(this->resp_));

        if (counters_) {
            stats::StatsManager::addValue(counters_->latency_, this->duration_.elapsedInUSec());
        }

        delete this;
    }

    nebula::cpp2::ErrorCode getSpaceVidLen(GraphSpaceID spaceId) {
        auto len = this->env_->schemaMan_->getSpaceVidLen(spaceId);
        if (!len.ok()) {
            return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        spaceVidLen_ = len.value();

        auto vIdType = this->env_->schemaMan_->getSpaceVidType(spaceId);
        if (!vIdType.ok()) {
            return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        isIntId_ = (vIdType.value() == meta::cpp2::PropertyType::INT64);

        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV>&& data);

    nebula::cpp2::ErrorCode
    doSyncPut(GraphSpaceID spaceId,
              PartitionID partId,
              std::vector<kvstore::KV>&& data);

    void doRemove(GraphSpaceID spaceId,
                  PartitionID partId,
                  std::vector<std::string>&& keys);

    void doRemoveRange(GraphSpaceID spaceId,
                       PartitionID partId,
                       const std::string& start,
                       const std::string& end);

    nebula::cpp2::ErrorCode writeResultTo(WriteResult code, bool isEdge);

    nebula::meta::cpp2::ColumnDef columnDef(std::string name,
                                            nebula::meta::cpp2::PropertyType type);

    void pushResultCode(nebula::cpp2::ErrorCode code,
                        PartitionID partId,
                        HostAddr leader = HostAddr("", 0));

    void handleErrorCode(nebula::cpp2::ErrorCode code,
                         GraphSpaceID spaceId,
                         PartitionID partId);

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId);

    void handleAsync(GraphSpaceID spaceId,
                     PartitionID partId,
                     nebula::cpp2::ErrorCode code);

    StatusOr<std::string> encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                       const std::vector<std::string>& propNames,
                                       const std::vector<Value>& props,
                                       WriteResult& wRet);

protected:
    StorageEnv*                                     env_{nullptr};
    const ProcessorCounters*                        counters_;

    RESP                                            resp_;
    folly::Promise<RESP>                            promise_;
    cpp2::ResponseCommon                            result_;

    time::Duration                                  duration_;
    std::vector<cpp2::PartitionResult>              codes_;
    std::mutex                                      lock_;
    int32_t                                         callingNum_{0};
    int32_t                                         spaceVidLen_;
    bool                                            isIntId_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
