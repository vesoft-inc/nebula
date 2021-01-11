/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/AdminSentences.h"
#include "util/SchemaUtil.h"

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

std::string ShowGroupsSentence::toString() const {
    return std::string("SHOW GROUPS");
}

std::string ShowZonesSentence::toString() const {
    return std::string("SHOW ZONES");
}

std::string SpaceOptItem::toString() const {
    switch (optType_) {
        case PARTITION_NUM:
            return folly::stringPrintf("partition_num = %ld", boost::get<int64_t>(optValue_));
        case REPLICA_FACTOR:
            return folly::stringPrintf("replica_factor = %ld", boost::get<int64_t>(optValue_));
        case VID_TYPE: {
            auto &typeDef = boost::get<meta::cpp2::ColumnTypeDef>(optValue_);
            return folly::stringPrintf("vid_type = %s",
                                       graph::SchemaUtil::typeToString(typeDef).c_str());
        }
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
        buf.pop_back();
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

std::string ShowConfigsSentence::toString() const {
    return std::string("SHOW CONFIGS ") + configItem_->toString();
}

std::string SetConfigSentence::toString() const {
    return std::string("SET CONFIGS ") + configItem_->toString();
}

std::string GetConfigSentence::toString() const {
    return std::string("GET CONFIGS ") + configItem_->toString();
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
        buf.pop_back();
    }
    return buf;
}

std::string CreateSnapshotSentence::toString() const {
    return "CREATE SNAPSHOT";
}

std::string DropSnapshotSentence::toString() const {
    return folly::stringPrintf("DROP SNAPSHOT %s", name_.get()->c_str());
}

std::string AddListenerSentence::toString() const {
    return "ADD LISTENER";
}

std::string RemoveListenerSentence::toString() const {
    return "REMOVE LISTENER";
}

std::string ShowListenerSentence::toString() const {
    return "SHOW LISTENER";
}

std::string AdminJobSentence::toString() const {
    switch (op_) {
    case meta::cpp2::AdminJobOp::ADD:
        return "add job";
    case meta::cpp2::AdminJobOp::SHOW_All:
        return "show jobs";
    case meta::cpp2::AdminJobOp::SHOW:
        return "show job";
    case meta::cpp2::AdminJobOp::STOP:
        return "stop job";
    case meta::cpp2::AdminJobOp::RECOVER:
        return "recover job";
    }
    LOG(FATAL) << "Unknown job operation " << static_cast<uint8_t>(op_);
}

meta::cpp2::AdminJobOp AdminJobSentence::getOp() const {
    return op_;
}

meta::cpp2::AdminCmd AdminJobSentence::getCmd() const {
    return cmd_;
}

const std::vector<std::string> &AdminJobSentence::getParas() const {
    return paras_;
}

void AdminJobSentence::addPara(const std::string& para) {
    paras_.emplace_back(para);
}

std::string ShowStatsSentence::toString() const {
    return folly::stringPrintf("SHOW STATS");
}

std::string ShowTSClientsSentence::toString() const {
    return "SHOW TEXT SEARCH CLIENTS";
}

std::string SignInTextServiceSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "SIGN IN TEXT SERVICE ";
    for (auto &client : clients_->clients()) {
        buf += "(";
        buf += client.get_host().host;
        buf += ":";
        buf += std::to_string(client.get_host().port);
        if (client.__isset.user && !client.get_user()->empty()) {
            buf += ", \"";
            buf += *client.get_user();
            buf += "\"";
        }
        if (client.__isset.pwd && !client.get_pwd()->empty()) {
            buf += ", \"";
            buf += *client.get_pwd();
            buf += "\"";
        }
        buf += ")";
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string SignOutTextServiceSentence::toString() const {
    return "SIGN OUT TEXT SERVICE";
}
}   // namespace nebula
