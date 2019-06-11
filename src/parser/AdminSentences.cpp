/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/AdminSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    switch (showType_) {
        case ShowType::kShowHosts:
            return std::string("SHOW HOSTS");
        case ShowType::kShowSpaces:
            return std::string("SHOW SPACES");
        case ShowType::kShowTags:
            return std::string("SHOW TAGS");
        case ShowType::kShowEdges:
            return std::string("SHOW EDGES");
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
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string AddHostsSentence::toString() const {
    return folly::stringPrintf("ADD HOSTS (%s) ", hosts_->toString().c_str());
}

std::string RemoveHostsSentence::toString() const {
    return folly::stringPrintf("REMOVE HOSTS (%s) ", hosts_->toString().c_str());
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
    if (!buf.empty()) {
        buf.resize(buf.size()-1);
    }
    return buf;
}


std::string CreateSpaceSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE SPACE ";
    buf += *spaceName_;
    if (spaceOpts_ != nullptr) {
        buf += "(";
        buf += spaceOpts_->toString();
        buf += ")";
    }
    return buf;
}

std::string DropSpaceSentence::toString() const {
    return folly::stringPrintf("DROP SPACE  %s", spaceName_.get()->c_str());
}

std::string DescribeSpaceSentence::toString() const {
    return folly::stringPrintf("DESCRIBE SPACE  %s", spaceName_.get()->c_str());
}

}   // namespace nebula
