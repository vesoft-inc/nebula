/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "base/Base.h"
#include "kvstore/Common.h"
#include "kvstore/KVEngine.h"

namespace nebula {
namespace kvstore {

class Part {
public:
    Part(GraphSpaceID spaceId,
         PartitionID partId,
         const std::string& walPath,
         KVEngine* engine)
        : spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {}

    virtual ~Part() = default;

    KVEngine* engine() {
        return engine_;
    }

    virtual void asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) = 0;

    virtual void asyncRemove(const std::string& key, KVCallback cb) = 0;

    virtual void asyncMultiRemove(std::vector<std::string> keys, KVCallback cb) = 0;

    virtual void asyncRemoveRange(const std::string& start,
                                  const std::string& end,
                                  KVCallback cb) = 0;

protected:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    KVEngine* engine_ = nullptr;
};

/**
 * Bypass raft, just write into storage when asyncMultiPut invoked.
 * */
class SimplePart final : public Part {
public:
    SimplePart(GraphSpaceID spaceId, PartitionID partId,
               const std::string& walPath, KVEngine* engine)
        : Part(spaceId, partId, walPath, engine) {}

    void asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) override;

    void asyncRemove(const std::string& key, KVCallback cb) override;

    void asyncMultiRemove(std::vector<std::string> keys, KVCallback cb) override;

    void asyncRemoveRange(const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;
};



}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_

