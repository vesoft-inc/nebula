/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_KVENGINE_H_
#define KVSTORE_KVENGINE_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace kvstore {

class WriteBatch {
public:
    virtual ~WriteBatch() = default;

    virtual ResultCode put(folly::StringPiece key, folly::StringPiece value) = 0;

    virtual ResultCode remove(folly::StringPiece key) = 0;

    // Remove all keys in the range [start, end)
    virtual ResultCode removeRange(folly::StringPiece start,
                                   folly::StringPiece end) = 0;
};


class KVEngine {
public:
    explicit KVEngine(GraphSpaceID spaceId)
                : spaceId_(spaceId) {}

    virtual ~KVEngine() = default;

    virtual void stop() = 0;

    // Retrieve the root path for the data
    // If the store is persistent, a valid path will be returned
    // Otherwise, nullptr will be returned
    virtual const char* getDataRoot() const = 0;

    virtual std::unique_ptr<WriteBatch> startBatchWrite() = 0;

    virtual ResultCode commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                                        bool disableWAL = true,
                                        bool sync = false) = 0;

    // Read a single key
    virtual ResultCode get(const std::string& key, std::string* value) = 0;

    // Read a list of keys, if key[i] does not exist, the i-th value in return value
    // would be Status::KeyNotFound
    virtual std::vector<Status> multiGet(const std::vector<std::string>& keys,
                                         std::vector<std::string>* values) = 0;

    // Get all results in range [start, end)
    virtual ResultCode range(const std::string& start,
                             const std::string& end,
                             std::unique_ptr<KVIterator>* iter) = 0;

    // Get all results with 'prefix' str as prefix.
    virtual ResultCode prefix(const std::string& prefix,
                              std::unique_ptr<KVIterator>* iter) = 0;

    // Get all results with 'prefix' str as prefix starting form 'start'
    virtual ResultCode rangeWithPrefix(const std::string& start,
                                       const std::string& prefix,
                                       std::unique_ptr<KVIterator>* iter) = 0;

    // Get all results in range [start, end)
    virtual ResultCode put(std::string key, std::string value) = 0;

    // Get all results with 'prefix' str as prefix.
    virtual ResultCode multiPut(std::vector<KV> keyValues) = 0;

    // Remove a single key
    virtual ResultCode remove(const std::string& key) = 0;

    // Remove a batch of keys
    virtual ResultCode multiRemove(std::vector<std::string> keys) = 0;

    // Remove range [start, end)
    virtual ResultCode removeRange(const std::string& start,
                                   const std::string& end) = 0;

    // Add partId into current storage engine.
    virtual void addPart(PartitionID partId) = 0;

    // Remove partId from current storage engine.
    virtual void removePart(PartitionID partId) = 0;


    // Return all partIds current storage engine holds.
    virtual std::vector<PartitionID> allParts() = 0;

    // Return total parts num
    virtual int32_t totalPartsNum() = 0;

    // Ingest sst files
    virtual ResultCode ingest(const std::vector<std::string>& files) = 0;

    // Set Config Option
    virtual ResultCode setOption(const std::string& configKey,
                                 const std::string& configValue) = 0;

    // Set DB Config Option
    virtual ResultCode setDBOption(const std::string& configKey,
                                   const std::string& configValue) = 0;

    virtual ResultCode compact() = 0;

    virtual ResultCode flush() = 0;

    virtual ResultCode createCheckpoint(const std::string& name) = 0;

protected:
    GraphSpaceID spaceId_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_KVENGINE_H_

