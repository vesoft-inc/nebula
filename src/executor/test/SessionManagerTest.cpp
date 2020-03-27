/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "graph/SessionManager.h"
#include "graph/GraphFlags.h"
#include "thread/GenericWorker.h"

using nebula::thread::GenericWorker;

namespace nebula {
namespace graph {

TEST(SessionManager, Basic) {
    auto sm = std::make_shared<SessionManager>();

    auto session = sm->createSession();
    ASSERT_NE(nullptr, session);
    ASSERT_NE(0, session->id());

    auto result = sm->findSession(session->id());
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(session.get(), result.value().get());
}

TEST(SessionManager, ExpiredSession) {
    FLAGS_session_idle_timeout_secs = 3;
    FLAGS_session_reclaim_interval_secs = 1;

    auto sm = std::make_shared<SessionManager>();

    auto worker = std::make_shared<GenericWorker>();
    ASSERT_TRUE(worker->start());

    auto idle = sm->createSession();
    auto active = sm->createSession();
    ASSERT_NE(nullptr, idle);
    ASSERT_NE(nullptr, active);
    ASSERT_NE(idle.get(), active.get());
    ASSERT_NE(idle->id(), active->id());

    // keep `active' active
    auto charger = [&] () {
        active->charge();
    };
    worker->addRepeatTask(1000/*ms*/, charger);

    // assert `idle' not expired
    auto check_not_expired = [&] () {
        auto result = sm->findSession(idle->id());
        ASSERT_TRUE(result.ok());
        ASSERT_NE(nullptr, result.value());
    };

    // assert `idle' has expired
    auto check_already_expired = [&] () {
        auto result = sm->findSession(idle->id());
        ASSERT_FALSE(result.ok());
    };

    auto timeout = FLAGS_session_idle_timeout_secs;
    auto future1 = worker->addDelayTask((timeout - 1) * 1000 - 50/*ms*/, check_not_expired);
    auto future2 = worker->addDelayTask((timeout + 1) * 1000 + 50/*ms*/, check_already_expired);

    std::move(future1).get();
    std::move(future2).get();

    auto result = sm->findSession(active->id());
    ASSERT_TRUE(result.ok());
    ASSERT_NE(nullptr, result.value());
    ASSERT_EQ(active.get(), result.value().get());

    worker->stop();
    worker->wait();
}

}   // namespace graph
}   // namespace nebula
