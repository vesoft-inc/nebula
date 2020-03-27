/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "session/Session.h"

namespace nebula {
namespace session {

Session::Session(int64_t id) {
    id_ = id;
}

std::shared_ptr<Session> Session::create(int64_t id) {
    return std::shared_ptr<Session>(new Session(id));
}

void Session::charge() {
    idleDuration_.reset();
}

uint64_t Session::idleSeconds() const {
    return idleDuration_.elapsedInSec();
}
}  // namespace session
}  // namespace nebula
