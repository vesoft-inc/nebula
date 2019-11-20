/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_HBASE_HBASESTORE_H_
#define KVSTORE_PLUGINS_HBASE_HBASESTORE_H_

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "kvstore/KVStore.h"
#include "kvstore/KVIterator.h"
#include "kvstore/plugins/hbase/HBaseClient.h"
#include <gtest/gtest_prod.h>

namespace nebula {
namespace kvstore {

class HBaseRangeIter : public KVIterator {
public:
    HBaseRangeIter(KVArrayIterator begin, KVArrayIterator end)
        : current_(begin)
        , begin_(begin)
        , end_(end) {}

    ~HBaseRangeIter()  = default;

    bool valid() const override {
        return current_ != end_;
    }

    void next() override {
        CHECK(current_ != end_);
        current_++;
    }

    void prev() override {
        CHECK(current_ != begin_);
        current_--;
    }

    folly::StringPiece key() const override {
        return folly::StringPiece(current_->first);
    }

    folly::StringPiece val() const override {
        return folly::StringPiece(current_->second);
    }

private:
    KVArrayIterator current_;
    KVArrayIterator begin_;
    KVArrayIterator end_;
};


class HBaseStore : public KVStore {
public:
    explicit HBaseStore(KVOptions options);

    ~HBaseStore() = default;

    // Connect to the HBase thrift server.
    void init();

    uint32_t capability() const override {
        return 0;
    }

    // Return the current leader
    ErrorOr<ResultCode, HostAddr> partLeader(GraphSpaceID spaceId, PartitionID partId) override {
        UNUSED(spaceId);
        UNUSED(partId);
        return {-1, -1};
    }

    ResultCode get(GraphSpaceID spaceId,
                   PartitionID  partId,
                   const std::string& key,
                   std::string* value) override;

    ResultCode multiGet(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::vector<std::string>& keys,
                        std::vector<std::string>* values) override;

    // Get all results in range [start, end)
    ResultCode range(GraphSpaceID spaceId,
                     PartitionID  partId,
                     const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter) override;

    // Get all results with prefix.
    ResultCode prefix(GraphSpaceID spaceId,
                      PartitionID  partId,
                      const std::string& prefix,
                      std::unique_ptr<KVIterator>* iter) override;

    // async batch put.
    void asyncMultiPut(GraphSpaceID spaceId,
                       PartitionID  partId,
                       std::vector<KV> keyValues,
                       KVCallback cb) override;

    void asyncRemove(GraphSpaceID spaceId,
                     PartitionID partId,
                     const std::string& key,
                     KVCallback cb) override;

    void asyncMultiRemove(GraphSpaceID spaceId,
                          PartitionID  partId,
                          std::vector<std::string> keys,
                          KVCallback cb) override;

    void asyncRemoveRange(GraphSpaceID spaceId,
                          PartitionID partId,
                          const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;

    void asyncRemovePrefix(GraphSpaceID spaceId,
                           PartitionID partId,
                           const std::string& prefix,
                           KVCallback cb) override;

    void asyncAtomicOp(GraphSpaceID,
                       PartitionID,
                       raftex::AtomicOp,
                       KVCallback) override {
        LOG(FATAL) << "Not supportted yet!";
    }

    ResultCode ingest(GraphSpaceID spaceId) override;

    int32_t allLeader(std::unordered_map<GraphSpaceID,
                                         std::vector<PartitionID>>& leaderIds) override;

    ErrorOr<ResultCode, std::shared_ptr<Part>> part(GraphSpaceID,
                                                    PartitionID) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

    ResultCode compact(GraphSpaceID) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

    ResultCode flush(GraphSpaceID) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

    ResultCode createCheckpoint(GraphSpaceID, const std::string&) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

    ResultCode dropCheckpoint(GraphSpaceID, const std::string&) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

    ResultCode setWriteBlocking(GraphSpaceID, bool) override {
        return ResultCode::ERR_UNSUPPORTED;
    }

private:
    std::string getRowKey(const std::string& key) {
        return key.substr(sizeof(PartitionID), key.size() - sizeof(PartitionID));
    }

    inline std::string spaceIdToTableName(GraphSpaceID spaceId);

    std::shared_ptr<const meta::SchemaProviderIf> getSchema(GraphSpaceID spaceId,
                                                            const std::string& key,
                                                            SchemaVer version = -1);

    std::string encode(GraphSpaceID spaceId,
                       const std::string& key,
                       KVMap& values);

    std::vector<KV> decode(GraphSpaceID spaceId,
                           const std::string& key,
                           std::string& encoded);

    ResultCode range(GraphSpaceID spaceId,
                     const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter);

    ResultCode prefix(GraphSpaceID spaceId,
                      const std::string& prefix,
                      std::unique_ptr<KVIterator>* iter);

    ResultCode multiRemove(GraphSpaceID spaceId,
                           std::vector<std::string>& keys);

private:
    KVOptions options_;

    std::unique_ptr<meta::SchemaManager> schemaMan_{nullptr};

    std::unique_ptr<HBaseClient> client_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_HBASE_HBASESTORE_H_

