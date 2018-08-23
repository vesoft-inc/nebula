/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_RAFTEXSERVICE_H_
#define RAFTEX_RAFTEXSERVICE_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "interface/gen-cpp2/RaftexService.h"
#include "thread/GenericThreadPool.h"

namespace vesoft {
namespace raftex {

class RaftPart;

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

    void stop();

    void askForVote(cpp2::AskForVoteResponse& resp,
                    const cpp2::AskForVoteRequest& req) override;

    void appendLog(cpp2::AppendLogResponse& resp,
                   const cpp2::AppendLogRequest& req) override;

    void addPartition(std::shared_ptr<RaftPart> part);
    void removePartition(std::shared_ptr<RaftPart> part);

private:
    RaftexService() = default;

    std::shared_ptr<RaftPart> findPart(GraphSpaceID spaceId,
                                       PartitionID partId);

private:
    std::unique_ptr<apache::thrift::ThriftServer> server_;
    std::unique_ptr<std::thread> serverThread_;
    uint32_t serverPort_;

    folly::RWSpinLock partsLock_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>,
                       std::shared_ptr<RaftPart>> parts_;
};

}  // namespace raftex
}  // namespace vesoft
#endif  // RAFTEX_RAFTEXSERVICE_H_

