/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"

#include "session/ClientSession.h"

namespace nebula {
namespace graph {

ClientSession::ClientSession(meta::cpp2::Session &&session, meta::MetaClient* metaClient) {
    session_ = std::move(session);
    metaClient_ = metaClient;
}

std::shared_ptr<ClientSession> ClientSession::create(meta::cpp2::Session &&session,
                                                     meta::MetaClient* metaClient) {
    return std::shared_ptr<ClientSession>(new ClientSession(std::move(session), metaClient));
}

void ClientSession::charge() {
    folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
    idleDuration_.reset();
    session_.set_update_time(time::WallClock::fastNowInMicroSec());
}

uint64_t ClientSession::idleSeconds() {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return idleDuration_.elapsedInSec();
}

}  // namespace graph
}  // namespace nebula
