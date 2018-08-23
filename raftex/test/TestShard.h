/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_TEST_TESTSHARD_H_
#define RAFTEX_TEST_TESTSHARD_H_

#include "base/Base.h"
#include "raftex/RaftPart.h"

namespace vesoft {
namespace raftex {
namespace test {

class TestShard : public RaftPart {
public:
    TestShard(PartitionID partId,
              HostAddr addr,
              std::vector<HostAddr>&& peers,
              const folly::StringPiece walRoot,
              BufferFlusher* flusher,
              std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
              std::shared_ptr<thread::GenericThreadPool> workers);

    void onLostLeadership(TermID term) override;
    void onElected(TermID term) override;

    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

private:
};

}  // namespace test
}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_TEST_TESTSHARD_H_

