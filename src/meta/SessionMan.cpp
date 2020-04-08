/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/SessionMan.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

std::unordered_map<std::string, sessionRec> SessionMan::globalSessions_ = {};

void SessionMan::addOrUpdateSession(const std::string& addr, int64_t sessionId, int64_t startTime,
                                    int64_t updateTime) {
    auto iter = globalSessions_.find(addr);
    // addr not exists
    if (iter == globalSessions_.end()) {
        sessionRec sessRec;
        sessRec[sessionId] = std::make_pair(startTime, updateTime);
        globalSessions_.emplace(addr, sessRec);
    } else {
        auto idMap = iter->second;
        auto idIter = idMap.find(sessionId);
        // SessionId not exists
        if (idIter == idMap.end()) {
            globalSessions_[addr][sessionId] = std::make_pair(startTime, updateTime);
        } else {
        // Update update time
            if (globalSessions_[addr][sessionId].first == startTime) {
                globalSessions_[addr][sessionId].second = updateTime;
            } else {
                // Previously invalid data
                globalSessions_[addr][sessionId] = std::make_pair(startTime, updateTime);
            }
        }
    }
}


void SessionMan::removeSession(const std::string& addr, int64_t sessionId) {
    auto iter = globalSessions_.find(addr);
    if (iter == globalSessions_.end()) {
        return;
    }

    auto idIter = iter->second.find(sessionId);
    if (idIter != iter->second.end()) {
        globalSessions_[addr].erase(idIter);
        if (globalSessions_[addr].empty()) {
            globalSessions_.erase(addr);
        }
    }
    return;
}


void SessionMan::removeInvalidSession(const std::unordered_set<std::string>& addrs,
                                      int64_t UpdateTime) {
    // Remove invalid session
    decltype(globalSessions_) globalSessions;

    for (auto gsIter = globalSessions_.begin(); gsIter != globalSessions_.end(); ++gsIter) {
        auto iterAddr = addrs.find(gsIter->first);

        // According to the update time interval, for graphd crashes
        if (iterAddr == addrs.end()) {
            for (auto siter = gsIter->second.begin();  siter != gsIter->second.end(); ++siter) {
                if (siter->second.second + 2 * FLAGS_heartbeat_interval_secs < UpdateTime) {
                    continue;
                } else {
                    globalSessions[gsIter->first][siter->first] =
                        std::make_pair(siter->second.first, siter->second.second);
                }
            }
        } else {
            // According to whether the update time is equal to updateTime, for session expires
            for (auto siter = gsIter->second.begin();  siter != gsIter->second.end(); ++siter) {
                if (siter->second.second == UpdateTime) {
                    globalSessions[gsIter->first][siter->first] =
                        std::make_pair(siter->second.first, siter->second.second);
                } else {
                    continue;
                }
            }
        }
    }
    globalSessions_ = std::move(globalSessions);
}

}   // namespace meta
}   // namespace nebula
