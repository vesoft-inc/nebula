/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/sessionMan/GlobalSessionProcessor.h"
#include "meta/SessionMan.h"

namespace nebula {
namespace meta {

void AddSessionProcessor::process(const cpp2::AddSessionReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::sessionLock());
    const auto& sessionItems = req.get_session_items();
    auto now = time::WallClock::fastNowInSec();

    for (auto& sessionItem : sessionItems) {
        auto& addr = sessionItem.get_addr();
        auto sessionId = sessionItem.get_session_id();
        auto startTime = sessionItem.get_start_time();
        SessionMan::addOrUpdateSession(addr, sessionId, startTime, now);
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
    return;
}


void RemoveSessionProcessor::process(const cpp2::RemoveSessionReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::sessionLock());
    const auto& sessionItems = req.get_session_items();

    for (auto& sessionItem : sessionItems) {
        auto& addr = sessionItem.get_addr();
        auto sessionId = sessionItem.get_session_id();
        SessionMan::removeSession(addr, sessionId);
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
    return;
}


void UpdateSessionProcessor::process(const cpp2::UpdateSessionReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::sessionLock());
    const auto& sessionItems = req.get_session_items();
    auto now = time::WallClock::fastNowInSec();

    std::unordered_set<std::string> addrs;
    for (auto& sessionItem : sessionItems) {
        auto& addr = sessionItem.get_addr();
        addrs.emplace(addr);
        auto sessionId = sessionItem.get_session_id();
        auto startTime = sessionItem.get_start_time();
        SessionMan::addOrUpdateSession(addr, sessionId, startTime, now);
    }

    SessionMan::removeInvalidSession(addrs, now);

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
    return;
}


void ListSessionsProcessor::process(const cpp2::ListSessionsReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::sessionLock());

    decltype(resp_.items) items;
    for (auto& sess : SessionMan::getGlobalSessions()) {
        auto& addr = sess.first;
        auto sessRec = sess.second;;
        for (auto& e : sessRec) {
            cpp2::SessionItem item;
            item.set_addr(addr);
            item.set_session_id(e.first);
            item.set_start_time(e.second.first);
            item.set_update_time(e.second.second);
            items.emplace_back(std::move(item));
        }
    }
    resp_.set_items(items);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
    return;
}

}   // namespace meta
}   // namespace nebula
