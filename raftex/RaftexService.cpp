/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/RaftexService.h"
#include <folly/ScopeGuard.h>
#include "raftex/RaftPart.h"

namespace vesoft {
namespace raftex {

std::shared_ptr<RaftexService> RaftexService::createService(
        std::shared_ptr<folly::IOThreadPoolExecutor> pool,
        uint16_t port) {
    auto svc = std::shared_ptr<RaftexService>(new RaftexService());

    svc->server_ = std::make_unique<apache::thrift::ThriftServer>();
    CHECK(svc->server_ != nullptr) << "Failed to create a thrift server";

    svc->server_->setInterface(svc);
    svc->server_->setPort(port);
    if (pool != nullptr) {
        svc->server_->setIOThreadPool(pool);
    }

    svc->serverThread_.reset(new std::thread([svc] {
        LOG(INFO) << "Starting the Raftex Service";

        svc->server_->setup();
        svc->serverPort_ = svc->server_->getAddress().getPort();
        SCOPE_EXIT {
            svc->server_->cleanUp();
        };
        svc->server_
           ->getEventBaseManager()
           ->getEventBase()
           ->loopForever();

        LOG(INFO) << "The Raftex Service stopped";
    }));

    return svc;
}


RaftexService::~RaftexService() {
    server_->stop();
    serverThread_->join();
}


std::shared_ptr<folly::IOThreadPoolExecutor>
RaftexService::getIOThreadPool() const {
    return server_->getIOThreadPool();
}


void RaftexService::stop() {
    server_->stop();
}


void RaftexService::addPartition(std::shared_ptr<RaftPart> part) {
    folly::RWSpinLock::WriteHolder wh(partsLock_);
    parts_.emplace(std::make_pair(part->spaceId(), part->partitionId()),
                   part);
}


void RaftexService::removePartition(std::shared_ptr<RaftPart> part) {
    folly::RWSpinLock::WriteHolder wh(partsLock_);
    parts_.erase(std::make_pair(part->spaceId(), part->partitionId()));
}


std::shared_ptr<RaftPart> RaftexService::findPart(GraphSpaceID spaceId,
                                                  PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(partsLock_);
    auto it = parts_.find(std::make_pair(spaceId, partId));
    if (it == parts_.end()) {
        // Part not found
        LOG(ERROR) << "Cannot find the part " << partId
                   << " in the graph space " << spaceId;
        return std::shared_ptr<RaftPart>();
    }

    // Otherwise, return the part pointer
    return it->second;
}


void RaftexService::askForVote(cpp2::AskForVoteResponse& resp,
                               const cpp2::AskForVoteRequest& req) {
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        return;
    }

    part->processAskForVoteRequest(req, resp);
}


void RaftexService::appendLog(cpp2::AppendLogResponse& resp,
                              const cpp2::AppendLogRequest& req) {
    auto part = findPart(req.get_space(), req.get_part());
    if (!part) {
        // Not found
        resp.set_error_code(cpp2::ErrorCode::E_UNKNOWN_PART);
        return;
    }

    part->processAppendLogRequest(req, resp);
}

}  // namespace raftex
}  // namespace vesoft

