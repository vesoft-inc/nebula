/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/RaftexService.h"

#include <folly/ScopeGuard.h>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/ssl/SSLConfig.h"
#include "kvstore/raftex/RaftPart.h"

namespace nebula {
namespace raftex {

/*******************************************************
 *
 * Implementation of RaftexService
 *
 ******************************************************/
std::shared_ptr<RaftexService> RaftexService::createService(
    std::shared_ptr<folly::IOThreadPoolExecutor> pool,
    std::shared_ptr<folly::Executor> workers,
    uint16_t port) {
  auto svc = std::shared_ptr<RaftexService>(new RaftexService());
  CHECK(svc != nullptr) << "Failed to create a raft service";

  svc->server_ = std::make_unique<apache::thrift::ThriftServer>();
  CHECK(svc->server_ != nullptr) << "Failed to create a thrift server";
  svc->server_->setInterface(svc);

  svc->initThriftServer(pool, workers, port);
  return svc;
}

RaftexService::~RaftexService() {}

bool RaftexService::start() {
  serverThread_.reset(new std::thread([&] { serve(); }));

  waitUntilReady();

  // start failed, reclaim resource
  if (status_.load() != STATUS_RUNNING) {
    waitUntilStop();
    return false;
  }

  return true;
}

void RaftexService::waitUntilReady() {
  while (status_.load() == STATUS_NOT_RUNNING) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void RaftexService::initThriftServer(std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                                     std::shared_ptr<folly::Executor> workers,
                                     uint16_t port) {
  LOG(INFO) << "Init thrift server for raft service, port: " << port;
  if (FLAGS_enable_ssl) {
    server_->setSSLConfig(nebula::sslContextConfig());
  }
  server_->setPort(port);
  server_->setIdleTimeout(std::chrono::seconds(0));
  if (pool != nullptr) {
    server_->setIOThreadPool(pool);
  }
  if (workers != nullptr) {
    server_->setThreadManager(
        std::dynamic_pointer_cast<apache::thrift::concurrency::ThreadManager>(workers));
  }
  server_->setStopWorkersOnStopListening(false);
}

bool RaftexService::setup() {
  try {
    server_->setup();
    serverPort_ = server_->getAddress().getPort();

    LOG(INFO) << "Starting the Raftex Service on " << serverPort_;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Setup the Raftex Service failed, error: " << e.what();
    return false;
  }

  return true;
}

void RaftexService::serve() {
  LOG(INFO) << "Starting the Raftex Service";

  if (!setup()) {
    status_.store(STATUS_SETUP_FAILED);
    return;
  }

  SCOPE_EXIT {
    server_->cleanUp();
  };

  status_.store(STATUS_RUNNING);
  LOG(INFO) << "Start the Raftex Service successfully";
  server_->getEventBaseManager()->getEventBase()->loopForever();

  status_.store(STATUS_NOT_RUNNING);
  LOG(INFO) << "The Raftex Service stopped";
}

std::shared_ptr<folly::IOThreadPoolExecutor> RaftexService::getIOThreadPool() const {
  return server_->getIOThreadPool();
}

std::shared_ptr<folly::Executor> RaftexService::getThreadManager() {
  return server_->getThreadManager();
}

void RaftexService::stop() {
  int expected = STATUS_RUNNING;
  if (!status_.compare_exchange_strong(expected, STATUS_NOT_RUNNING)) {
    return;
  }

  // stop service
  LOG(INFO) << "Stopping the raftex service on port " << serverPort_;
  {
    folly::RWSpinLock::WriteHolder wh(partsLock_);
    for (auto& p : parts_) {
      p.second->stop();
    }
    parts_.clear();
    LOG(INFO) << "All partitions have stopped";
  }
  server_->stop();
}

void RaftexService::waitUntilStop() {
  if (serverThread_) {
    serverThread_->join();
    serverThread_.reset();
    server_.reset();
    LOG(INFO) << "Server thread has stopped. Service on port " << serverPort_
              << " is ready to be destroyed";
  }
}

void RaftexService::addPartition(std::shared_ptr<RaftPart> part) {
  // todo(doodle): If we need to start both listener and normal replica on same
  // hosts, this class need to be aware of type.
  folly::RWSpinLock::WriteHolder wh(partsLock_);
  auto spaceId = part->spaceId();
  auto path = folly::stringPrintf("%s/nebula/%d", part->path().c_str(), spaceId);
  parts_.emplace(std::make_tuple(spaceId, part->partitionId(), path), part);
}

void RaftexService::removePartition(std::shared_ptr<RaftPart> part) {
  using FType = decltype(folly::makeFuture());
  using FTValype = typename FType::value_type;
  // Stop the partition
  folly::makeFuture()
      .thenValue([this, &part](FTValype) {
        folly::RWSpinLock::WriteHolder wh(partsLock_);
        auto spaceId = part->spaceId();
        auto path = folly::stringPrintf("%s/nebula/%d", part->path().c_str(), spaceId);
        parts_.erase(std::make_tuple(spaceId, part->partitionId(), path));
      })
      // the part->stop() would wait for requestOnGoing_ in Host, and the requestOnGoing_ will
      // release in response in ioThreadPool,this may cause deadlock, so doing it in another
      // threadpool to avoid this condition
      .via(folly::getGlobalCPUExecutor())
      .thenValue([part](FTValype) { part->stop(); })
      .wait();
}

std::shared_ptr<RaftPart> RaftexService::findPart(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  const std::string& path) {
  folly::RWSpinLock::ReadHolder rh(partsLock_);
  std::string p;
  for (auto iter = parts_.begin(); iter != parts_.end(); iter++) {
    LOG(INFO) << "Space " << std::get<0>(iter->first) << " Part " << std::get<1>(iter->first)
              << " Path " << std::get<2>(iter->first);
    p = std::get<2>(iter->first);
  }

  auto it = parts_.find(std::make_tuple(spaceId, partId, p));
  if (it == parts_.end()) {
    // Part not found
    LOG(WARNING) << "Cannot find the part " << partId << " in the graph space " << spaceId
                 << ", path " << path;
    return std::shared_ptr<RaftPart>();
  }

  // Otherwise, return the part pointer
  return it->second;
}

void RaftexService::getState(cpp2::GetStateResponse& resp, const cpp2::GetStateRequest& req) {
  LOG(INFO) << "Get State Space " << req.get_space() << ", Part " << req.get_part() << ", Path "
            << req.get_path();
  auto part = findPart(req.get_space(), req.get_part(), req.get_path());
  if (part != nullptr) {
    part->getState(resp);
  } else {
    resp.term_ref() = -1;
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART;
  }
}

void RaftexService::askForVote(cpp2::AskForVoteResponse& resp, const cpp2::AskForVoteRequest& req) {
  LOG(INFO) << "Ask For Vote Space " << req.get_space() << ", Part " << req.get_part() << ", Path "
            << req.get_path();
  auto part = findPart(req.get_space(), req.get_part(), req.get_path());
  if (!part) {
    // Not found
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART;
    return;
  }

  part->processAskForVoteRequest(req, resp);
}

void RaftexService::appendLog(cpp2::AppendLogResponse& resp, const cpp2::AppendLogRequest& req) {
  LOG(INFO) << "Append Log " << req.get_space() << ", Part " << req.get_part() << ", Path "
            << req.get_path();
  auto part = findPart(req.get_space(), req.get_part(), req.get_path());
  if (!part) {
    // Not found
    LOG(WARNING) << "Append Log Cannot find the part " << req.get_part() << " in the graph space "
                 << req.get_space() << ", path " << req.get_path();
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART;
    return;
  }

  part->processAppendLogRequest(req, resp);
}

void RaftexService::sendSnapshot(cpp2::SendSnapshotResponse& resp,
                                 const cpp2::SendSnapshotRequest& req) {
  LOG(INFO) << "Send Snapshot " << req.get_space() << ", Part " << req.get_part() << ", Path "
            << req.get_path();
  auto part = findPart(req.get_space(), req.get_part(), req.get_path());
  if (!part) {
    // Not found
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART;
    return;
  }

  part->processSendSnapshotRequest(req, resp);
}

void RaftexService::async_eb_heartbeat(
    std::unique_ptr<apache::thrift::HandlerCallback<cpp2::HeartbeatResponse>> callback,
    const cpp2::HeartbeatRequest& req) {
  LOG(INFO) << "Heartbeat " << req.get_space() << ", Part " << req.get_part() << ", Path "
            << req.get_path();
  cpp2::HeartbeatResponse resp;
  auto part = findPart(req.get_space(), req.get_part(), req.get_path());
  if (!part) {
    // Not found
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_RAFT_UNKNOWN_PART;
    callback->result(resp);
    return;
  }
  part->processHeartbeatRequest(req, resp);
  callback->result(resp);
}

}  // namespace raftex
}  // namespace nebula
