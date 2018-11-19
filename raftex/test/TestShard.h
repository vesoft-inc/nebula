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

class RaftexService;

namespace test {

class TestShard : public RaftPart {
public:
    TestShard(
        size_t idx,
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
            becomeLeaderCB);

    std::shared_ptr<RaftexService> getService() const {
        return service_;
    }

    size_t index() const {
        return idx_;
    }

    void onLostLeadership(TermID term) override;
    void onElected(TermID term) override;

    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

private:
    const size_t idx_;
    std::shared_ptr<RaftexService> service_;

    std::function<void (size_t idx, const char*, TermID)>
        leadershipLostCB_;
    std::function<void (size_t idx, const char*, TermID)>
        becomeLeaderCB_;
};

}  // namespace test
}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_TEST_TESTSHARD_H_

