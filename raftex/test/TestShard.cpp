/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/test/TestShard.h"
#include "raftex/RaftexService.h"
#include "raftex/FileBasedWal.h"
#include "raftex/BufferFlusher.h"

namespace vesoft {
namespace raftex {
namespace test {

TestShard::TestShard(size_t idx,
                     std::shared_ptr<RaftexService> svc,
                     PartitionID partId,
                     HostAddr addr,
                     std::vector<HostAddr>&& peers,
                     const folly::StringPiece walRoot,
                     BufferFlusher* flusher,
                     std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                     std::shared_ptr<thread::GenericThreadPool> workers,
                     std::function<void (size_t idx, const char*, TermID)>
                        leadershipLostCB,
                     std::function<void (size_t idx, const char*, TermID)>
                        becomeLeaderCB)
        : RaftPart(1,   // clusterId
                   1,   // spaceId
                   partId,
                   addr,
                   std::move(peers),
                   walRoot,
                   flusher,
                   ioPool,
                   workers)
        , idx_(idx)
        , service_(svc)
        , leadershipLostCB_(leadershipLostCB)
        , becomeLeaderCB_(becomeLeaderCB) {
}


void TestShard::onLostLeadership(TermID term) {
    if (leadershipLostCB_) {
        leadershipLostCB_(idx_, idStr(), term);
    }
}


void TestShard::onElected(TermID term) {
    if (becomeLeaderCB_) {
        becomeLeaderCB_(idx_, idStr(), term);
    }
}


bool TestShard::commitLogs(std::unique_ptr<LogIterator> iter) {
    return true;
}

}  // namespace test
}  // namespace raftex
}  // namespace vesoft

