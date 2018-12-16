/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/ClientSession.h"


namespace nebula {
namespace graph {

ClientSession::ClientSession(int64_t id) {
    id_ = id;
}

std::shared_ptr<ClientSession> ClientSession::create(int64_t id) {
    // return std::make_shared<ClientSession>(id);
    // `std::make_shared' cannot access ClientSession's construtor
    return std::shared_ptr<ClientSession>(new ClientSession(id));
}

void ClientSession::charge() {
    idleDuration_.reset();
}

uint64_t ClientSession::idleSeconds() const {
    return idleDuration_.elapsedInSec();
}

}   // namespace graph
}   // namespace nebula
