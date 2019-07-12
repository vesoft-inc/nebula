/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_RAFTEXSERVICE_H_
#define RAFTEX_RAFTEXSERVICE_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "gen-cpp2/RaftexService.h"
#include "thread/GenericThreadPool.h"

namespace nebula {
namespace raftex {

class RaftPart;
class IOThreadPoolObserver;


class RaftexService : public cpp2::RaftexServiceSvIf {
public:
    static std::shared_ptr<RaftexService> createService(
        std::shared_ptr<folly::IOThreadPoolExecutor> pool,
        uint16_t port = 0);
    virtual ~RaftexService();

    uint32_t getServerPort() const {
        return serverPort_;
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> getIOThreadPool() const;

    bool start();
    void stop();

    void askForVote(cpp2::AskForVoteResponse& resp,
                    const cpp2::AskForVoteRequest& req) override;

    void appendLog(cpp2::AppendLogResponse& resp,
                   const cpp2::AppendLogRequest& req) override;

    void addPartition(std::shared_ptr<RaftPart> part);
    void removePartition(std::shared_ptr<RaftPart> part);

private:
    void initThriftServer(std::shared_ptr<folly::IOThreadPoolExecutor> pool, uint16_t port = 0);
    bool setup();
    void serve();

    enum RaftServiceStatus{
        STATUS_INIT             = 0,
        STATUS_START_FAILED     = 1,
        STATUS_RUNNING          = 2,
        STATUS_STOPPING         = 3,
        STATUS_STOPPED          = 4
    };
    void notifyRaftServiceStatus(RaftServiceStatus status);

    // Block until the service is ready to serve
    void waitUntilReady();
    void waitUntilStop();

    RaftexService() = default;

    std::shared_ptr<RaftPart> findPart(GraphSpaceID spaceId,
                                       PartitionID partId);

private:
    std::unique_ptr<apache::thrift::ThriftServer> server_;
    std::unique_ptr<std::thread> serverThread_;
    uint32_t serverPort_;

    std::mutex readyMutex_;
    std::condition_variable readyCV_;
    RaftServiceStatus status_{STATUS_INIT};

    folly::RWSpinLock partsLock_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>,
                       std::shared_ptr<RaftPart>> parts_;
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_RAFTEXSERVICE_H_

