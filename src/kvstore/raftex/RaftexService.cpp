/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "kvstore/raftex/RaftexService.h"
#include <folly/ScopeGuard.h>
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


RaftexService::~RaftexService() {
}


bool RaftexService::start() {
    serverThread_.reset(new std::thread([&] {serve();}));

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
    server_->setPort(port);
    server_->setIdleTimeout(std::chrono::seconds(0));
    if (pool != nullptr) {
        server_->setIOThreadPool(pool);
    }
    if (workers != nullptr) {
        server_->setThreadManager(
                std::dynamic_pointer_cast<
                        apache::thrift::concurrency::ThreadManager>(workers));
    }
    server_->setStopWorkersOnStopListening(false);
}



bool RaftexService::setup() {
    try {
        server_->setup();
        serverPort_ = server_->getAddress().getPort();

        LOG(INFO) << "Starting the Raftex Service on " << serverPort_;
    }
    catch (const std::exception &e) {
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


std::shared_ptr<folly::IOThreadPoolExecutor>
RaftexService::getIOThreadPool() const {
    return server_->getIOThreadPool();
}

std::shared_ptr<folly::Executor> RaftexService::getThreadManager() {
    return server_->getThreadManager();
}

void RaftexService::stop() {
    if (status_.load() != STATUS_RUNNING) {
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
        LOG(INFO) << "Server thread has stopped. Service on port "
                  << serverPort_ << " is ready to be destroyed";
    }
}


void RaftexService::addPartition(std::shared_ptr<RaftPart> part) {
    // todo(doodle): If we need to start both listener and normal replica on same hosts,
    // this class need to be aware of type.
    folly::RWSpinLock::WriteHolder wh(partsLock_);
    parts_.emplace(std::make_pair(part->spaceId(), part->partitionId()),
                   part);
}


void RaftexService::removePartition(std::shared_ptr<RaftPart> part) {
    folly::RWSpinLock::WriteHolder wh(partsLock_);
    parts_.erase(std::make_pair(part->spaceId(), part->partitionId()));
    // Stop the partition
    part->stop();
}


std::shared_ptr<RaftPart> RaftexService::findPart(
        GraphSpaceID spaceId,
        PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(partsLock_);
    auto it = parts_.find(std::make_pair(spaceId, partId));
    if (it == parts_.end()) {
        // Part not found
        LOG_EVERY_N(WARNING, 100) << "Cannot find the part " << partId
                                  << " in the graph space " << spaceId;
        return std::shared_ptr<RaftPart>();
    }

    // Otherwise, return the part pointer
    return it->second;
}


void RaftexService::askForVote(
        cpp2::AskForVoteResponse& resp,
        const cpp2::AskForVoteRequest& req) {
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        return;
    }

    part->processAskForVoteRequest(req, resp);
}


void RaftexService::appendLog(
        cpp2::AppendLogResponse& resp,
        const cpp2::AppendLogRequest& req) {
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        return;
    }

    part->processAppendLogRequest(req, resp);
}

void RaftexService::sendSnapshot(
        cpp2::SendSnapshotResponse& resp,
        const cpp2::SendSnapshotRequest& req) {
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        return;
    }

    part->processSendSnapshotRequest(req, resp);
}

void RaftexService::async_eb_heartbeat(
        std::unique_ptr<apache::thrift::HandlerCallback<cpp2::HeartbeatResponse>> callback,
        const cpp2::HeartbeatRequest& req) {
    cpp2::HeartbeatResponse resp;
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        callback->result(resp);
        return;
    }
    part->processHeartbeatRequest(req, resp);
    callback->result(resp);
}

}  // namespace raftex
}  // namespace nebula

