/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "parser/AdminSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    switch (showType_) {
        case ShowType::kShowHosts:
            return std::string("SHOW HOSTS");
        case ShowType::kShowSpaces:
            return std::string("SHOW SPACES");
        case ShowType::kShowUsers:
            return std::string("SHOW USERS");
        case ShowType::kShowUser:
            return folly::stringPrintf("SHOW USER %s", name_.get()->data());
        case ShowType::kShowRoles:
            return folly::stringPrintf("SHOW ROLES IN %s", name_.get()->data());
        case ShowType::kUnknown:
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}


std::string HostList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &host : hosts_) {
        buf += network::NetworkUtils::intToIPv4(host->first);
        buf += ":";
        buf += std::to_string(host->second);
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    return buf;
}


std::string AddHostsSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ADD HOSTS (";
    buf += hosts_->toString();
    buf += ") ";
    return buf;
}


std::string RemoveHostsSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "REMOVE HOSTS (";
    buf += hosts_->toString();
    buf += ") ";
    return buf;
}


std::string SpaceOptItem::toString() const {
    switch (optType_) {
        case PARTITION_NUM:
            return folly::stringPrintf("partition_num = %ld", boost::get<int64_t>(optValue_));
        case REPLICA_FACTOR:
            return folly::stringPrintf("replica_factor = %ld", boost::get<int64_t>(optValue_));
        default:
             FLOG_FATAL("Space parameter illegal");
    }
    return "Unknown";
}


std::string SpaceOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : items_) {
        buf += item->toString();
        buf += ",";
    }
    buf.resize(buf.size()-1);
    return buf;
}


std::string CreateSpaceSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE SPACE ";
    buf += *spaceName_;
    buf += "(";
    buf += spaceOpts_->toString();
    buf += ") ";
    return buf;
}


std::string DropSpaceSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DROP SPACE ";
    buf += *spaceName_;
    return buf;
}

}   // namespace nebula
