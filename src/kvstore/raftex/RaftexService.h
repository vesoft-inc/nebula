/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_RAFTEXSERVICE_H_
#define RAFTEX_RAFTEXSERVICE_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/RaftexService.h"
#include <folly/RWSpinLock.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

namespace nebula {
namespace raftex {

class RaftPart;
class IOThreadPoolObserver;


class RaftexService : public cpp2::RaftexServiceSvIf {
public:
    static std::shared_ptr<RaftexService> createService(
        std::shared_ptr<folly::IOThreadPoolExecutor> pool,
        std::shared_ptr<folly::Executor> workers,
        uint16_t port = 0);
    virtual ~RaftexService();

    uint32_t getServerPort() const {
        return serverPort_;
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> getIOThreadPool() const;

    std::shared_ptr<folly::Executor> getThreadManager();

    bool start();
    void stop();
    void waitUntilStop();

    void askForVote(cpp2::AskForVoteResponse& resp,
                    const cpp2::AskForVoteRequest& req) override;

    void appendLog(cpp2::AppendLogResponse& resp,
                   const cpp2::AppendLogRequest& req) override;

    void sendSnapshot(
        cpp2::SendSnapshotResponse& resp,
        const cpp2::SendSnapshotRequest& req) override;

    void async_eb_heartbeat(
        std::unique_ptr<apache::thrift::HandlerCallback<cpp2::HeartbeatResponse>> callback,
        const cpp2::HeartbeatRequest& req) override;

    void addPartition(std::shared_ptr<RaftPart> part);
    void removePartition(std::shared_ptr<RaftPart> part);

    std::shared_ptr<RaftPart> findPart(GraphSpaceID spaceId,
                                       PartitionID partId);

private:
    void initThriftServer(std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                          std::shared_ptr<folly::Executor> workers,
                          uint16_t port = 0);
    bool setup();
    void serve();

    // Block until the service is ready to serve
    void waitUntilReady();

    RaftexService() = default;

private:
    std::unique_ptr<apache::thrift::ThriftServer> server_;
    std::unique_ptr<std::thread> serverThread_;
    uint32_t serverPort_;

    enum RaftServiceStatus{
        STATUS_NOT_RUNNING      = 0,
        STATUS_SETUP_FAILED     = 1,
        STATUS_RUNNING          = 2
    };
    std::atomic_int status_{STATUS_NOT_RUNNING};

    folly::RWSpinLock partsLock_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>,
                       std::shared_ptr<RaftPart>> parts_;
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_RAFTEXSERVICE_H_

