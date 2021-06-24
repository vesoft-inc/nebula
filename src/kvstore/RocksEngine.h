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
#include <rocksdb/utilities/backupable_db.h>
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
                const std::string& walPath = "",
                std::shared_ptr<rocksdb::MergeOperator> mergeOp = nullptr,
                std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory = nullptr,
                bool readonly = false);

    ~RocksEngine() {
        LOG(INFO) << "Release rocksdb on " << dataPath_;
    }

    void stop() override;

    // return path to a spaceId, e.g. "/DataPath/nebula/spaceId", usally it should contain
    // two subdir: data and wal.
    const char* getDataRoot() const override {
        return dataPath_.c_str();
    }

    const char* getWalRoot() const override {
        return walPath_.c_str();
    }

    std::unique_ptr<WriteBatch> startBatchWrite() override;

    nebula::cpp2::ErrorCode
    commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                     bool disableWAL,
                     bool sync,
                     bool wait) override;

    /*********************
     * Data retrieval
     ********************/
    nebula::cpp2::ErrorCode get(const std::string& key, std::string* value) override;

    std::vector<Status> multiGet(const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) override;

    nebula::cpp2::ErrorCode
    range(const std::string& start,
          const std::string& end,
          std::unique_ptr<KVIterator>* iter) override;

    nebula::cpp2::ErrorCode
    prefix(const std::string& prefix, std::unique_ptr<KVIterator>* iter) override;

    nebula::cpp2::ErrorCode
    rangeWithPrefix(const std::string& start,
                    const std::string& prefix,
                    std::unique_ptr<KVIterator>* iter) override;

    /*********************
     * Data modification
     ********************/
    nebula::cpp2::ErrorCode put(std::string key, std::string value) override;

    nebula::cpp2::ErrorCode multiPut(std::vector<KV> keyValues) override;

    nebula::cpp2::ErrorCode remove(const std::string& key) override;

    nebula::cpp2::ErrorCode multiRemove(std::vector<std::string> keys) override;

    nebula::cpp2::ErrorCode
    removeRange(const std::string& start, const std::string& end) override;

    /*********************
     * Non-data operation
     ********************/
    void addPart(PartitionID partId) override;

    void removePart(PartitionID partId) override;

    std::vector<PartitionID> allParts() override;

    int32_t totalPartsNum() override;

    nebula::cpp2::ErrorCode ingest(const std::vector<std::string>& files,
                                   bool verifyFileChecksum = false) override;

    nebula::cpp2::ErrorCode
    setOption(const std::string& configKey, const std::string& configValue) override;

    nebula::cpp2::ErrorCode
    setDBOption(const std::string& configKey, const std::string& configValue) override;

    nebula::cpp2::ErrorCode compact() override;

    nebula::cpp2::ErrorCode flush() override;

    nebula::cpp2::ErrorCode backup() override;

    /*********************
     * Checkpoint operation
     ********************/
    nebula::cpp2::ErrorCode createCheckpoint(const std::string& path) override;

    ErrorOr<nebula::cpp2::ErrorCode, std::string>
    backupTable(const std::string& path,
                const std::string& tablePrefix,
                std::function<bool(const folly::StringPiece& key)> filter) override;

private:
    std::string partKey(PartitionID partId);

    void openBackupEngine(GraphSpaceID spaceId);

private:
    GraphSpaceID spaceId_;
    std::string dataPath_;
    std::string walPath_;
    std::unique_ptr<rocksdb::DB> db_{nullptr};
    std::string backupPath_;
    std::unique_ptr<rocksdb::BackupEngine> backupDb_{nullptr};
    int32_t partsNum_ = -1;
};

}   // namespace kvstore
}   // namespace nebula

#endif   // KVSTORE_ROCKSENGINE_H_
