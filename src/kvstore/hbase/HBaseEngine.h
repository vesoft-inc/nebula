/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_HBASEENGINE_H_
#define KVSTORE_HBASEENGINE_H_

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "kvstore/hbase/HBaseClient.h"
#include "kvstore/KVEngine.h"
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


class HBaseEngine : public KVEngine {
public:
    HBaseEngine(GraphSpaceID spaceId,
                const HostAddr& hbaseServer,
                meta::SchemaManager* schemaMan);

    ~HBaseEngine();

    ResultCode get(const std::string& key,
                   std::string* value) override;

    ResultCode multiGet(const std::vector<std::string>& keys,
                        std::vector<std::string>* values) override;

    ResultCode put(std::string key,
                   std::string value) override;

    ResultCode multiPut(std::vector<KV> keyValues) override;

    ResultCode range(const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter) override;

    ResultCode prefix(const std::string& prefix,
                      std::unique_ptr<KVIterator>* iter) override;

    ResultCode remove(const std::string& key) override;

    ResultCode multiRemove(std::vector<std::string> keys) override;

    ResultCode removeRange(const std::string& start,
                           const std::string& end) override;

    void addPart(PartitionID partId) override;

    void removePart(PartitionID partId) override;

    std::vector<PartitionID> allParts() override;

    int32_t totalPartsNum() override;

    ResultCode ingest(const std::vector<std::string>& files) override;

    ResultCode setOption(const std::string& config_key,
                         const std::string& config_value) override;

    ResultCode setDBOption(const std::string& config_key,
                           const std::string& config_value) override;

    ResultCode compactAll() override;

private:
    std::string getRowKey(const std::string& key) {
        return key.substr(sizeof(PartitionID), key.size() - sizeof(PartitionID));
    }

    std::shared_ptr<const meta::SchemaProviderIf> getSchema(const std::string& key,
                                                            SchemaVer version = -1);

    std::string encode(const std::string& key, KVMap& values);

    std::vector<KV> decode(const std::string& key, std::string& encoded);

private:
    std::string tableName_;

    std::unique_ptr<HBaseClient> client_{nullptr};

    meta::SchemaManager* schemaMan_ = nullptr;

    int32_t partsNum_ = -1;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_HBASEENGINE_H_

