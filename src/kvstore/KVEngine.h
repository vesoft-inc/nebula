/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_KVENGINE_H_
#define KVSTORE_KVENGINE_H_

#include "base/Base.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace kvstore {

class KVEngine {
public:
    explicit KVEngine(GraphSpaceID spaceId)
                : spaceId_(spaceId) {}

    virtual ~KVEngine() = default;

    virtual ResultCode get(const std::string& key,
                           std::string* value) = 0;

    virtual ResultCode multiGet(const std::vector<std::string>& keys,
                                std::vector<std::string>* values) = 0;

    virtual ResultCode put(std::string key,
                           std::string value) = 0;

    virtual ResultCode multiPut(std::vector<KV> keyValues) = 0;

    /**
     * Get all results in range [start, end)
     * */
    virtual ResultCode range(const std::string& start,
                             const std::string& end,
                             std::unique_ptr<KVIterator>* iter) = 0;

    /**
     * Get all results with 'prefix' str as prefix.
     * */
    virtual ResultCode prefix(const std::string& prefix,
                              std::unique_ptr<KVIterator>* iter) = 0;

    virtual ResultCode remove(const std::string& key) = 0;

    virtual ResultCode multiRemove(std::vector<std::string> keys) = 0;

    /**
     * Remove range [start, end)
     * */
    virtual ResultCode removeRange(const std::string& start,
                                   const std::string& end) = 0;
    /**
     * Add partId into current storage engine.
     * */
    virtual void addPart(PartitionID partId) = 0;

    /**
     * Remove partId from current storage engine.
     * */
    virtual void removePart(PartitionID partId) = 0;

    /**
     * Return all partIds current storage engine holded.
     * */
    virtual std::vector<PartitionID> allParts() = 0;

    /**
     * Return total parts num
     * */
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

