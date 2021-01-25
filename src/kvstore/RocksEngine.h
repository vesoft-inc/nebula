/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_ROCKSENGINE_H_
#define KVSTORE_ROCKSENGINE_H_

#include <gtest/gtest_prod.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
#include "common/base/Base.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace kvstore {

class RocksRangeIter : public KVIterator {
public:
    RocksRangeIter(rocksdb::Iterator* iter, rocksdb::Slice start, rocksdb::Slice end)
        : iter_(iter), start_(start), end_(end) {}

    ~RocksRangeIter() = default;

    bool valid() const override {
        return !!iter_ && iter_->Valid() && (iter_->key().compare(end_) < 0);
    }

    void next() override {
        iter_->Next();
    }

    void prev() override {
        iter_->Prev();
    }

    folly::StringPiece key() const override {
        return folly::StringPiece(iter_->key().data(), iter_->key().size());
    }

    folly::StringPiece val() const override {
        return folly::StringPiece(iter_->value().data(), iter_->value().size());
    }

private:
    std::unique_ptr<rocksdb::Iterator> iter_;
    rocksdb::Slice start_;
    rocksdb::Slice end_;
};

class RocksPrefixIter : public KVIterator {
public:
    RocksPrefixIter(rocksdb::Iterator* iter, rocksdb::Slice prefix)
        : iter_(iter), prefix_(prefix) {}

    ~RocksPrefixIter() = default;

    bool valid() const override {
        return !!iter_ && iter_->Valid() && (iter_->key().starts_with(prefix_));
    }

    void next() override {
        iter_->Next();
    }

    void prev() override {
        iter_->Prev();
    }

    folly::StringPiece key() const override {
        return folly::StringPiece(iter_->key().data(), iter_->key().size());
    }

    folly::StringPiece val() const override {
        return folly::StringPiece(iter_->value().data(), iter_->value().size());
    }

protected:
    std::unique_ptr<rocksdb::Iterator> iter_;
    rocksdb::Slice prefix_;
};

/**************************************************************************
 *
 * An implementation of KVEngine based on Rocksdb
 *
 *************************************************************************/
class RocksEngine : public KVEngine {
    FRIEND_TEST(RocksEngineTest, SimpleTest);

public:
    RocksEngine(GraphSpaceID spaceId,
                int32_t vIdLen,
                const std::string& dataPath,
                std::shared_ptr<rocksdb::MergeOperator> mergeOp = nullptr,
                std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory = nullptr);

    ~RocksEngine() {
        LOG(INFO) << "Release rocksdb on " << dataPath_;
    }

    void stop() override;

    const char* getDataRoot() const override {
        return dataPath_.c_str();
    }

    std::unique_ptr<WriteBatch> startBatchWrite() override;

    ResultCode commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                                bool disableWAL,
                                bool sync) override;

    /*********************
     * Data retrieval
     ********************/
    ResultCode get(const std::string& key, std::string* value) override;

    std::vector<Status> multiGet(const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) override;

    ResultCode range(const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter) override;

    ResultCode prefix(const std::string& prefix, std::unique_ptr<KVIterator>* iter) override;

    ResultCode rangeWithPrefix(const std::string& start,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter) override;

    /*********************
     * Data modification
     ********************/
    ResultCode put(std::string key, std::string value) override;

    ResultCode multiPut(std::vector<KV> keyValues) override;

    ResultCode remove(const std::string& key) override;

    ResultCode multiRemove(std::vector<std::string> keys) override;

    ResultCode removeRange(const std::string& start, const std::string& end) override;

    /*********************
     * Non-data operation
     ********************/
    void addPart(PartitionID partId) override;

    void removePart(PartitionID partId) override;

    std::vector<PartitionID> allParts() override;

    int32_t totalPartsNum() override;

    ResultCode ingest(const std::vector<std::string>& files) override;

    ResultCode setOption(const std::string& configKey, const std::string& configValue) override;

    ResultCode setDBOption(const std::string& configKey, const std::string& configValue) override;

    ResultCode compact() override;

    ResultCode flush() override;

    /*********************
     * Checkpoint operation
     ********************/
    ResultCode createCheckpoint(const std::string& path) override;

    ErrorOr<ResultCode, std::string> backupTable(
        const std::string& path,
        const std::string& tablePrefix,
        std::function<bool(const folly::StringPiece& key)> filter) override;

private:
    std::string partKey(PartitionID partId);

private:
    std::string dataPath_;
    std::unique_ptr<rocksdb::DB> db_{nullptr};
    int32_t partsNum_ = -1;
};

}   // namespace kvstore
}   // namespace nebula
#endif   // KVSTORE_ROCKSENGINE_H_
