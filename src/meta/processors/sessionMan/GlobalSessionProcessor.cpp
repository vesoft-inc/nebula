/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/sessionMan/GlobalSessionProcessor.h"
#include "meta/SessionMan.h"

DECLARE_int32(heartbeat_interval_secs);

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

        auto it = SessionMan::globalSessions_.find(addr);
        // addr not exists
        if (it == SessionMan::globalSessions_.end()) {
            sessionRec sessRec;
            sessRec[sessionId] = std::make_pair(startTime, now);
            SessionMan::globalSessions_.emplace(addr, sessRec);
        } else {
            auto idmap = it->second;
            auto itid = idmap.find(sessionId);
            // SessionId not exists
            if (itid == idmap.end()) {
                SessionMan::globalSessions_[addr][sessionId] = std::make_pair(startTime, now);
            } else {
                // Update update_time
                if (SessionMan::globalSessions_[addr][sessionId].first == startTime) {
                    SessionMan::globalSessions_[addr][sessionId].second = now;
                } else {
                    // Previously invalid data
                    SessionMan::globalSessions_[addr][sessionId] = std::make_pair(startTime, now);
                }
            }
        }
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

        auto it = SessionMan::globalSessions_.find(addr);
        if (it == SessionMan::globalSessions_.end()) {
            continue;
        }
        auto itid = SessionMan::globalSessions_[addr].find(sessionId);
        if (itid == SessionMan::globalSessions_[addr].end()) {
            continue;
        }
        SessionMan::globalSessions_[addr].erase(itid);
        if (SessionMan::globalSessions_[addr].empty()) {
            SessionMan::globalSessions_.erase(it);
        }
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

        auto it = SessionMan::globalSessions_.find(addr);
        // addr not exists
        if (it == SessionMan::globalSessions_.end()) {
            sessionRec sessRec;
            sessRec[sessionId] = std::make_pair(startTime, now);
            SessionMan::globalSessions_.emplace(addr, sessRec);
        } else {
            auto idmap = it->second;
            auto itid = idmap.find(sessionId);
            // SessionId not exists
            if (itid == idmap.end()) {
                SessionMan::globalSessions_[addr][sessionId] = std::make_pair(startTime, now);
            } else {
                // Update update_time
                if (SessionMan::globalSessions_[addr][sessionId].first == startTime) {
                    SessionMan::globalSessions_[addr][sessionId].second = now;
                } else {
                    // Previously invalid data
                    SessionMan::globalSessions_[addr][sessionId] = std::make_pair(startTime, now);
                }
            }
        }
    }

    // Remove invalid session
    std::unordered_set<std::string>::iterator iter = addrs.begin();
    while (iter!= addrs.end()) {
        auto itsec = SessionMan::globalSessions_.find(*iter);
        if (itsec != SessionMan::globalSessions_.end()) {
            for (auto& rec : itsec->second) {
                if (rec.second.second != now) {
                    SessionMan::globalSessions_[*iter].erase(rec.first);
                    if (SessionMan::globalSessions_[*iter].empty()) {
                        SessionMan::globalSessions_.erase(*iter);
                    }
                }
            }
        }
        ++iter;
    }

    // Remove graph crash session
    std::unordered_map<std::string, std::unordered_map<int64_t,
        std::pair<int64_t, int64_t>>>::iterator gsiter = SessionMan::globalSessions_.begin();
    while (gsiter != SessionMan::globalSessions_.end()) {
        std::unordered_map<int64_t, std::pair<int64_t, int64_t>>::iterator siter =
            gsiter->second.begin();
        while (siter != gsiter->second.end()) {
            if (siter->second.second + 2 * FLAGS_heartbeat_interval_secs < now) {
                auto& addr = gsiter->first;
                SessionMan::globalSessions_[addr].erase(siter->first);
                if (SessionMan::globalSessions_[addr].empty()) {
                    SessionMan::globalSessions_.erase(addr);
                }
            }
            ++siter;
        }
        ++gsiter;
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
    return;
}


void ListSessionsProcessor::process(const cpp2::ListSessionsReq& req) {
    UNUSED(req);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::sessionLock());
    decltype(resp_.items) items;
    for (auto& sess : SessionMan::globalSessions_) {
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
