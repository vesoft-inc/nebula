/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PART_H_
#define KVSTORE_PART_H_

#include "base/Base.h"
#include "raftex/RaftPart.h"
#include "kvstore/Common.h"
#include "kvstore/KVEngine.h"

namespace nebula {
namespace kvstore {

class Part : public raftex::RaftPart {
public:
    Part(GraphSpaceID spaceId,
         PartitionID partId,
         HostAddr localAddr,
         const std::string& walPath,
         KVEngine* engine,
         std::shared_ptr<folly::IOThreadPoolExecutor> pool,
         std::shared_ptr<thread::GenericThreadPool> workers,
         wal::BufferFlusher* flusher);

    virtual ~Part() {
        LOG(INFO) << idStr_ << "~Part()";
    }

    KVEngine* engine() {
        return engine_;
    }

    void asyncPut(folly::StringPiece key, folly::StringPiece value, KVCallback cb);
    void asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb);

    void asyncRemove(folly::StringPiece key, KVCallback cb);
    void asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb);
    void asyncRemovePrefix(folly::StringPiece prefix, KVCallback cb);
    void asyncRemoveRange(folly::StringPiece start,
                          folly::StringPiece end,
                          KVCallback cb);

    /**
     * Methods inherited from RaftPart
     */
    LogID lastCommittedLogId() override;

    void onLostLeadership(TermID term) override;

    void onElected(TermID term) override;

    std::string compareAndSet(const std::string& log) override;

    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

protected:
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::string walPath_;
    KVEngine* engine_ = nullptr;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PART_H_

