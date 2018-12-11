/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_STORAGEENGINE_H_
#define STORAGE_STORAGEENGINE_H_

#include "base/Base.h"
#include "storage/ResultCode.h"

namespace vesoft {
namespace vgraph {
namespace storage {

class StorageIter {
public:
    virtual ~StorageIter()  = default;

    virtual bool valid() = 0;

    virtual void next() = 0;

    virtual void prev() = 0;

    virtual folly::StringPiece key() = 0;

    virtual folly::StringPiece val() = 0;
};

class StorageEngine {
public:
    StorageEngine(GraphSpaceID spaceId)
        : spaceId_(spaceId) {}

    virtual ~StorageEngine() = default;

    /**
     * Initialize the engine. Register partIds into the engine.
     * */
    virtual void registerParts(const std::vector<PartitionID>& ids) = 0;

    /**
     * Do some cleanup work before dtor.
     * */
    virtual void cleanup() = 0;
 
    virtual ResultCode get(PartitionID partId,
                           const std::string& key,
                           std::string& value) = 0;

    virtual ResultCode put(PartitionID partId,
                           std::string key,
                           std::string value) = 0;

    virtual ResultCode multiPut(PartitionID partId, std::vector<KV> keyValues) = 0;
    
    /**
     * Get all results in range [start, end)
     * */
    virtual ResultCode range(PartitionID partId,
                             const std::string& start,
                             const std::string& end,
                             std::shared_ptr<StorageIter>& iter) = 0;

    /**
     * Get all results with 'prefix' str as prefix.
     * */
    virtual ResultCode prefix(PartitionID partId,
                              const std::string& prefix,
                              std::shared_ptr<StorageIter>& iter) = 0;

private:
    GraphSpaceID spaceId_;
};

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft
#endif  // STORAGE_STORAGEENGINE_H_

