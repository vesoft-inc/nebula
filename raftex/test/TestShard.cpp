/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/test/TestShard.h"
#include "raftex/FileBasedWal.h"
#include "raftex/BufferFlusher.h"

namespace vesoft {
namespace raftex {
namespace test {

TestShard::TestShard(PartitionID partId,
                     HostAddr addr,
                     std::vector<HostAddr>&& peers,
                     const folly::StringPiece walRoot,
                     BufferFlusher* flusher,
                     std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                     std::shared_ptr<thread::GenericThreadPool> workers)
        : RaftPart(1,   // clusterId
                   1,   // spaceId
                   partId,
                   addr,
                   std::move(peers),
                   walRoot,
                   flusher,
                   ioPool,
                   workers) {
}


void TestShard::onLostLeadership(TermID term) {
    LOG(INFO) << idStr() << "Term " << term
              << " is passed. I'm not the leader any more";
}


void TestShard::onElected(TermID term) {
    LOG(INFO) << idStr() << "I'm elected as the leader for the term "
              << term;
}


bool TestShard::commitLogs(std::unique_ptr<LogIterator> iter) {
    return true;
}

}  // namespace test
}  // namespace raftex
}  // namespace vesoft

