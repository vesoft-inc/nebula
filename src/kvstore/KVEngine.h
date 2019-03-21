/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_KVENGINE_H_
#define KVSTORE_KVENGINE_H_

#include "base/Base.h"
#include "base/ErrorOr.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace kvstore {

class WriteBatch {
public:
    virtual ~WriteBatch() = default;

    virtual ResultCode put(folly::StringPiece key, folly::StringPiece value) = 0;
    virtual ResultCode putFromLog(folly::StringPiece log) = 0;

    virtual ResultCode multiPut(const std::vector<KV>& keyValues) = 0;
    virtual ResultCode multiPutFromLog(folly::StringPiece log) = 0;

    virtual ResultCode remove(folly::StringPiece key) = 0;
    virtual ResultCode removeFromLog(folly::StringPiece log) = 0;

    virtual ResultCode multiRemove(const std::vector<std::string>& keys) = 0;
    virtual ResultCode multiRemoveFromLog(folly::StringPiece log) = 0;

    virtual ResultCode removePrefix(folly::StringPiece prefix) = 0;
    virtual ResultCode removePrefixFromLog(folly::StringPiece log) = 0;

    // Remove all keys in the range [start, end)
    virtual ResultCode removeRange(folly::StringPiece start,
                                   folly::StringPiece end) = 0;
    virtual ResultCode removeRangeFromLog(folly::StringPiece log) = 0;
};


class KVEngine {
public:
    explicit KVEngine(GraphSpaceID spaceId)
                : spaceId_(spaceId) {}

    virtual ~KVEngine() = default;

    // Retrieve the root path for the data
    // If the store is persistent, a valid path will be returned
    // Otherwise, nullptr will be returned
    virtual const char* getDataRoot() const = 0;

    virtual std::unique_ptr<WriteBatch> startBatchWrite() = 0;
    virtual ResultCode commitBatchWrite(std::unique_ptr<WriteBatch> batch) = 0;

    /**
     * Read a single key
     **/
    virtual ErrorOr<ResultCode, std::string> get(folly::StringPiece key) = 0;

    /**
     * Read a list of keys
     **/
    virtual ErrorOr<ResultCode, std::vector<std::string>>
    multiGet(const std::vector<std::string>& keys) = 0;

    /**
     * Get all results in range [start, end)
     **/
    virtual ErrorOr<ResultCode, std::unique_ptr<KVIterator>>
    range(folly::StringPiece start, folly::StringPiece end) = 0;

    /**
     * Get all results with 'prefix' str as prefix.
     **/
    virtual ErrorOr<ResultCode, std::unique_ptr<KVIterator>>
    prefix(folly::StringPiece prefix) = 0;

    /**
     * Put a single key/value pair
     **/
    virtual ResultCode put(folly::StringPiece key, folly::StringPiece value) = 0;

    /**
     * Put multiple key/value pairs
     **/
    virtual ResultCode multiPut(const std::vector<KV>& keyValues) = 0;

    /**
     * Remove a single key
     **/
    virtual ResultCode remove(folly::StringPiece key) = 0;

    /**
     * Remove a batch of keys
     **/
    virtual ResultCode multiRemove(const std::vector<std::string>& keys) = 0;

    /**
     * Remove keys matching the given prefix
     **/
    virtual ResultCode removePrefix(folly::StringPiece prefix) = 0;

    /**
     * Remove range [start, end)
     **/
    virtual ResultCode removeRange(folly::StringPiece start,
                                   folly::StringPiece end) = 0;
    /**
     * Add partId into current storage engine.
     **/
    virtual void addPart(PartitionID partId) = 0;

    /**
     * Remove partId from current storage engine.
     **/
    virtual void removePart(PartitionID partId) = 0;

    /**
     * Return all partIds current storage engine holded.
     **/
    virtual std::vector<PartitionID> allParts() = 0;

    /**
     * Return total parts num
     **/
    virtual int32_t totalPartsNum() = 0;

    /**
     * Ingest sst files
     */
    virtual ResultCode ingest(const std::vector<std::string>& files) = 0;

    /**
     * Set Config Option
     */
    virtual ResultCode setOption(const std::string& configKey,
                                 const std::string& configValue) = 0;

    /**
     * Set DB Config Option
     */
    virtual ResultCode setDBOption(const std::string& configKey,
                                   const std::string& configValue) = 0;

    virtual ResultCode compactAll() = 0;

protected:
    GraphSpaceID spaceId_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_KVENGINE_H_

