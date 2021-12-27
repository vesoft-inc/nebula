/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef RAFTEX_RAFTEXSERVICE_H_
#define RAFTEX_RAFTEXSERVICE_H_

#include <folly/RWSpinLock.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "interface/gen-cpp2/RaftexService.h"

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
  virtual ~RaftexService() = default;

  uint32_t getServerPort() const {
    return serverPort_;
  }

  std::shared_ptr<folly::IOThreadPoolExecutor> getIOThreadPool() const;

  std::shared_ptr<folly::Executor> getThreadManager();

  bool start();
  void stop();
  void waitUntilStop();

  void askForVote(cpp2::AskForVoteResponse& resp, const cpp2::AskForVoteRequest& req) override;

  void getState(cpp2::GetStateResponse& resp, const cpp2::GetStateRequest& req) override;

  void appendLog(cpp2::AppendLogResponse& resp, const cpp2::AppendLogRequest& req) override;

  void sendSnapshot(cpp2::SendSnapshotResponse& resp,
                    const cpp2::SendSnapshotRequest& req) override;

  void async_eb_heartbeat(
      std::unique_ptr<apache::thrift::HandlerCallback<cpp2::HeartbeatResponse>> callback,
      const cpp2::HeartbeatRequest& req) override;

  void addPartition(std::shared_ptr<RaftPart> part);
  void removePartition(std::shared_ptr<RaftPart> part);

  std::shared_ptr<RaftPart> findPart(GraphSpaceID spaceId, PartitionID partId);

 private:
  RaftexService() = default;

  void initThriftServer(std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                        std::shared_ptr<folly::Executor> workers,
                        uint16_t port = 0);

 private:
  std::unique_ptr<apache::thrift::ThriftServer> server_;
  std::unique_ptr<std::thread> serverThread_;
  uint32_t serverPort_;

  folly::RWSpinLock partsLock_;
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::shared_ptr<RaftPart>> parts_;
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_RAFTEXSERVICE_H_
