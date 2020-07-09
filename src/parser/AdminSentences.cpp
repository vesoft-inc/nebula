/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/AdminSentences.h"

namespace nebula {

std::string ShowHostsSentence::toString() const {
    return std::string("SHOW HOSTS");
}

std::string ShowSpacesSentence::toString() const {
    return std::string("SHOW SPACES");
}

std::string ShowCreateSpaceSentence::toString() const {
    return folly::stringPrintf("SHOW CREATE SPACE %s", name_.get()->c_str());
}

std::string ShowPartsSentence::toString() const {
    return std::string("SHOW PARTS");
}

std::string ShowUsersSentence::toString() const {
    return std::string("SHOW USERS");
}

std::string ShowRolesSentence::toString() const {
    return folly::stringPrintf("SHOW ROLES IN %s", name_.get()->c_str());
}

std::string ShowSnapshotsSentence::toString() const {
    return std::string("SHOW SNAPSHOTS");
}

std::string ShowCharsetSentence::toString() const {
    return std::string("SHOW CHARSET");
}

std::string ShowCollationSentence::toString() const {
    return std::string("SHOW COLLATION");
}

std::string SpaceOptItem::toString() const {
    switch (optType_) {
        case PARTITION_NUM:
            return folly::stringPrintf("partition_num = %ld", boost::get<int64_t>(optValue_));
        case REPLICA_FACTOR:
            return folly::stringPrintf("replica_factor = %ld", boost::get<int64_t>(optValue_));
        case VID_SIZE:
            return folly::stringPrintf("vid_size = %ld", boost::get<int64_t>(optValue_));
        case CHARSET:
            return folly::stringPrintf("charset = %s", boost::get<std::string>(optValue_).c_str());
        case COLLATE:
            return folly::stringPrintf("collate = %s", boost::get<std::string>(optValue_).c_str());
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
    return folly::stringPrintf("DROP SPACE %s", spaceName_.get()->c_str());
}


std::string DescribeSpaceSentence::toString() const {
    return folly::stringPrintf("DESCRIBE SPACE %s", spaceName_.get()->c_str());
}

std::string ConfigRowItem::toString() const {
    return "";
}

std::string ConfigSentence::toString() const {
    switch (subType_) {
        case SubType::kShow:
            return std::string("SHOW CONFIGS ") + configItem_->toString();
        case SubType::kSet:
            return std::string("SET CONFIGS ") + configItem_->toString();
        case SubType::kGet:
            return std::string("GET CONFIGS ") + configItem_->toString();
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}

std::string BalanceSentence::toString() const {
    switch (subType_) {
        case SubType::kLeader:
            return std::string("BALANCE LEADER");
        default:
            FLOG_FATAL("Type illegal");
    }
    return "Unknown";
}

std::string HostList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &host : hosts_) {
        buf += host->host;
        buf += ":";
        buf += std::to_string(host->port);
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string CreateSnapshotSentence::toString() const {
    return "CREATE SNAPSHOT";
}

std::string DropSnapshotSentence::toString() const {
    return folly::stringPrintf("DROP SNAPSHOT %s", name_.get()->c_str());
}

}   // namespace nebula
