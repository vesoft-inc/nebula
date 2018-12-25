/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_ROCKSDBENGINE_H_
#define KVSTORE_ROCKSDBENGINE_H_

#include <gtest/gtest_prod.h>
#include <rocksdb/db.h>
#include "base/Base.h"
#include "kvstore/StorageEngine.h"

namespace nebula {
namespace kvstore {

class RocksdbRangeIter : public StorageIter {
public:
    RocksdbRangeIter(rocksdb::Iterator* iter, rocksdb::Slice start, rocksdb::Slice end)
        : iter_(iter)
        , start_(start)
        , end_(end) {}

    ~RocksdbRangeIter()  = default;

    bool valid() override {
        return !!iter_ && iter_->Valid() && (iter_->key().compare(end_) < 0); 
    }

    void next() override {
        iter_->Next();
    }

    void prev() override {
        iter_->Prev();
    }

    folly::StringPiece key() override {
        return folly::StringPiece(iter_->key().data(), iter_->key().size());
    }

    folly::StringPiece val() override {
        return folly::StringPiece(iter_->value().data(), iter_->value().size());
    }

private:
    std::unique_ptr<rocksdb::Iterator> iter_;
    rocksdb::Slice start_;
    rocksdb::Slice end_;
};

class RocksdbPrefixIter : public StorageIter {
public:
    RocksdbPrefixIter(rocksdb::Iterator* iter, rocksdb::Slice prefix)
        : iter_(iter)
        , prefix_(prefix) {}

    ~RocksdbPrefixIter()  = default;

    bool valid() override {
        return !!iter_ && iter_->Valid() && (iter_->key().starts_with(prefix_)); 
    }

    void next() override {
        iter_->Next();
    }

    void prev() override {
        iter_->Prev();
    }

    folly::StringPiece key() override {
        return folly::StringPiece(iter_->key().data(), iter_->key().size());
    }

    folly::StringPiece val() override {
        return folly::StringPiece(iter_->value().data(), iter_->value().size());
    }

private:
    std::unique_ptr<rocksdb::Iterator> iter_;
    rocksdb::Slice prefix_;
};

class RocksdbEngine : public StorageEngine {
    FRIEND_TEST(RocksdbEngineTest, SimpleTest);
public:
    RocksdbEngine(GraphSpaceID spaceId, const std::string& dataPath);

    ~RocksdbEngine();

    ResultCode get(const std::string& key,
                   std::string& value) override;

    ResultCode put(std::string key,
                   std::string value) override;

    ResultCode multiPut(std::vector<KV> keyValues) override;

    ResultCode range(const std::string& start,
                     const std::string& end,
                     std::unique_ptr<StorageIter>& iter) override;

    ResultCode prefix(const std::string& prefix,
                      std::unique_ptr<StorageIter>& iter) override;

private:
    std::string  dataPath_;
    std::unique_ptr<rocksdb::DB> db_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_ROCKSDBENGINE_H_

