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
#include "dataman/RowSetWriter.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "storage/Collector.h"
#include "meta/SchemaManager.h"
#include "time/Duration.h"
#include "stats/StatsManager.h"
#include "stats/Stats.h"

DECLARE_bool(enable_multi_versions);

namespace nebula {
namespace storage {

using PartCode = std::pair<PartitionID, kvstore::ResultCode>;

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(kvstore::KVStore* kvstore,
                           meta::SchemaManager* schemaMan,
                           stats::Stats* stats = nullptr)
            : kvstore_(kvstore)
            , schemaMan_(schemaMan)
            , stats_(stats) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    virtual void onFinished() {
        stats::Stats::addStatsValue(stats_,
                                    this->result_.get_failed_codes().empty(),
                                    this->duration_.elapsedInUSec());
        this->result_.set_latency_in_us(this->duration_.elapsedInUSec());
        this->result_.set_failed_codes(this->codes_);
        this->resp_.set_result(std::move(this->result_));
        this->promise_.setValue(std::move(this->resp_));
        delete this;
    }

    kvstore::ResultCode doGetFirstRecord(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         std::string& key,
                                         std::string* value);

    void doPut(GraphSpaceID spaceId, PartitionID partId, std::vector<kvstore::KV> data);

    kvstore::ResultCode doSyncPut(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  std::vector<kvstore::KV> data);

    void doRemove(GraphSpaceID spaceId, PartitionID partId, std::vector<std::string> keys);

    void doRemoveRange(GraphSpaceID spaceId,
                       PartitionID partId,
                       const std::string& start,
                       const std::string& end);

    kvstore::ResultCode doRange(GraphSpaceID spaceId, PartitionID partId, const std::string& start,
                                const std::string& end, std::unique_ptr<kvstore::KVIterator>* iter);

    kvstore::ResultCode doRange(GraphSpaceID spaceId, PartitionID partId,
                                std::string&& start, std::string&& end,
                                std::unique_ptr<kvstore::KVIterator>* iter) = delete;

    kvstore::ResultCode doPrefix(GraphSpaceID spaceId, PartitionID partId,
                                 const std::string& prefix,
                                 std::unique_ptr<kvstore::KVIterator>* iter);

    kvstore::ResultCode doPrefix(GraphSpaceID spaceId, PartitionID partId,
                                 std::string&& prefix,
                                 std::unique_ptr<kvstore::KVIterator>* iter) = delete;

    kvstore::ResultCode doRangeWithPrefix(GraphSpaceID spaceId, PartitionID partId,
                                          const std::string& start, const std::string& prefix,
                                          std::unique_ptr<kvstore::KVIterator>* iter);

    kvstore::ResultCode doRangeWithPrefix(GraphSpaceID spaceId, PartitionID partId,
                                          std::string&& start, std::string&& prefix,
                                          std::unique_ptr<kvstore::KVIterator>* iter) = delete;

    nebula::cpp2::ColumnDef columnDef(std::string name, nebula::cpp2::SupportedType type) {
        nebula::cpp2::ColumnDef column;
        column.set_name(std::move(name));
        nebula::cpp2::ValueType vType;
        vType.set_type(type);
        column.set_type(std::move(vType));
        return column;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code);

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId) {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(code);
            thriftRet.set_part_id(partId);
            codes_.emplace_back(std::move(thriftRet));
        }
    }

    void pushResultCode(cpp2::ErrorCode code, PartitionID partId, HostAddr leader) {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(code);
            thriftRet.set_part_id(partId);
            thriftRet.set_leader(toThriftHost(leader));
            codes_.emplace_back(std::move(thriftRet));
        }
    }

    void handleErrorCode(kvstore::ResultCode code, GraphSpaceID spaceId, PartitionID partId) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                handleLeaderChanged(spaceId, partId);
            } else {
                pushResultCode(to(code), partId);
            }
        }
    }

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId) {
        auto addrRet = kvstore_->partLeader(spaceId, partId);
        if (ok(addrRet)) {
            auto leader = value(std::move(addrRet));
            this->pushResultCode(cpp2::ErrorCode::E_LEADER_CHANGED, partId, leader);
        } else {
            LOG(ERROR) << "Fail to get part leader, spaceId: " << spaceId
                       << ", partId: " << partId << ", ResultCode: " << error(addrRet);
            this->pushResultCode(to(error(addrRet)), partId);
        }
    }

    nebula::cpp2::HostAddr toThriftHost(const HostAddr& host) {
        nebula::cpp2::HostAddr tHost;
        tHost.set_ip(host.first);
        tHost.set_port(host.second);
        return tHost;
    }

    StatusOr<IndexValues> collectIndexValues(RowReader* reader,
                                             const std::vector<nebula::cpp2::ColumnDef>& cols);

    void collectProps(RowReader* reader, const std::vector<PropContext>& props,
                      Collector* collector);

    void handleAsync(GraphSpaceID spaceId, PartitionID partId, kvstore::ResultCode code);

protected:
    kvstore::KVStore*                               kvstore_{nullptr};
    meta::SchemaManager*                            schemaMan_{nullptr};
    stats::Stats*                                   stats_{nullptr};
    RESP                                            resp_;
    folly::Promise<RESP>                            promise_;
    cpp2::ResponseCommon                            result_;

    time::Duration                                  duration_;
    std::vector<cpp2::ResultCode>                   codes_;
    std::mutex                                      lock_;
    int32_t                                         callingNum_{0};
};

}  // namespace storage
}  // namespace nebula

#include "storage/BaseProcessor.inl"

#endif  // STORAGE_BASEPROCESSOR_H_
