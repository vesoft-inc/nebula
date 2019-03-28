/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "parser/AdminSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    std::string buf;
    switch (showKind_) {
        case ShowKind::kShowHosts:
            buf = "SHOW HOSTS";
            break;
        case ShowKind::kUnknown:
        default:
            LOG(FATAL) << "Show Sentence kind illegal: " << showKind_;
            break;
    }
    return buf;
}

std::string HostList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &host : hostStrs_) {
        buf += *host;
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
    char buf[256];
    switch (optType_) {
        case PARTITION_NUM:
            snprintf(buf, sizeof(buf), "partition_num = %ld", boost::get<int64_t>(optValue_));
            break;
        case REPLICA_FACTOR:
            snprintf(buf, sizeof(buf), "replica_factor = %ld", boost::get<int64_t>(optValue_));
            break;
    }
    return buf;
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
