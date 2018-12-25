/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "base/Base.h"
#include "kvstore/StorageEngine.h"
#include "kvstore/include/KVStore.h"

namespace nebula {
namespace kvstore {

class Part {
public:
    Part(GraphSpaceID spaceId,
         PartitionID partId,
         const std::string& walPath,
         StorageEngine* engine)
        : spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {}

    virtual ~Part() = default;

    StorageEngine* engine() {
        return engine_;
    }

    virtual ResultCode asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) = 0;

protected:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    StorageEngine* engine_ = nullptr;
};

/**
 * Bypass raft, just write into storage when asyncMultiPut invoked.
 * */
class SimplePart final : public Part {
public:
    SimplePart(GraphSpaceID spaceId, PartitionID partId,
               const std::string& walPath, StorageEngine* engine) 
        : Part(spaceId, partId, walPath, engine) {}

    ResultCode asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) override;
};



}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_

