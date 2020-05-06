/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_BASEPROCESSOR_H_
#define STORAGE_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/KVStore.h"
#include "meta/SchemaManager.h"
#include "meta/IndexManager.h"
#include "meta/SchemaManager.h"
#include "time/Duration.h"
#include "stats/StatsManager.h"
#include "stats/Stats.h"
#include "codec/RowReader.h"

namespace nebula {
namespace storage {

using PartCode = std::pair<PartitionID, kvstore::ResultCode>;

struct StorageEnv {
    kvstore::KVStore*                               kvstore_{nullptr};
    meta::SchemaManager*                            schemaMan_{nullptr};
    meta::IndexManager*                             indexMan_{nullptr};
};

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

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    kvstore::ResultCode doSyncPut(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  std::vector<kvstore::KV> data);

    void doRemove(GraphSpaceID spaceId, PartitionID partId, std::vector<std::string> keys);

    cpp2::ErrorCode to(kvstore::ResultCode code);

    nebula::meta::cpp2::ColumnDef columnDef(std::string name,
                                            nebula::meta::cpp2::PropertyType type);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId, HostAddr leader);

    void handleErrorCode(kvstore::ResultCode code, GraphSpaceID spaceId, PartitionID partId);

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId);

    void handleAsync(GraphSpaceID spaceId, PartitionID partId, kvstore::ResultCode code);

    StatusOr<std::string> encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                       const std::vector<std::string>& propNames,
                                       const std::vector<Value>& props);

protected:
    StorageEnv*                                     env_{nullptr};
    stats::Stats*                                   stats_{nullptr};
    RESP                                            resp_;
    folly::Promise<RESP>                            promise_;
    cpp2::ResponseCommon                            result_;

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
