/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/AdminSentences.h"
#include <sstream>
#include "util/SchemaUtil.h"
#include <thrift/lib/cpp/util/EnumUtils.h>

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
        case OptionType::PARTITION_NUM:
            return folly::stringPrintf("partition_num = %ld", boost::get<int64_t>(optValue_));
        case OptionType::REPLICA_FACTOR:
            return folly::stringPrintf("replica_factor = %ld", boost::get<int64_t>(optValue_));
        case OptionType::VID_TYPE: {
            auto &typeDef = boost::get<meta::cpp2::ColumnTypeDef>(optValue_);
            return folly::stringPrintf("vid_type = %s",
                                       graph::SchemaUtil::typeToString(typeDef).c_str());
        }
        case OptionType::CHARSET:
            return folly::stringPrintf("charset = %s", boost::get<std::string>(optValue_).c_str());
        case OptionType::COLLATE:
            return folly::stringPrintf("collate = %s", boost::get<std::string>(optValue_).c_str());
        case OptionType::ATOMIC_EDGE:
            return folly::stringPrintf("atomic_edge = %s", getAtomicEdge() ? "true" : "false");
        case OptionType::GROUP_NAME:
            return "";
    }
    DLOG(FATAL) << "Space parameter illegal";
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
    if (groupName_ != nullptr) {
        buf += " ON ";
        buf += *groupName_;
    }
    if (comment_ != nullptr) {
        buf += " comment = \"";
        buf += *comment_;
        buf += "\"";
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
    std::string buf;
    buf.reserve(128);
    if (module_ != meta::cpp2::ConfigModule::ALL) {
        buf += apache::thrift::util::enumNameSafe(module_);
        buf += ":";
    }
    if (name_ != nullptr) {
        buf += *name_;
    }
    if (value_ != nullptr) {
        buf += " = ";
        buf += value_->toString();
    }
    if (updateItems_ != nullptr) {
        buf += " = ";
        buf += "{";
        buf += updateItems_->toString();
        buf += "}";
    }
    return buf;
}

std::string ShowConfigsSentence::toString() const {
    std::string buf;
    buf += "SHOW CONFIGS ";
    if (configItem_ != nullptr) {
        configItem_->toString();
    }
    return buf;
}

std::string SetConfigSentence::toString() const {
    return std::string("UPDATE CONFIGS ") + configItem_->toString();
}

std::string GetConfigSentence::toString() const {
    return std::string("GET CONFIGS ") + configItem_->toString();
}

std::string BalanceSentence::toString() const {
    switch (subType_) {
        case SubType::kUnknown:
            return "Unknown";
        case SubType::kLeader:
            return "BALANCE LEADER";
        case SubType::kData: {
            if (hostDel_ == nullptr) {
                return "BALANCE DATA";
            } else {
                std::stringstream ss;
                ss << "BALANCE DATA REMOVE ";
                ss << hostDel_->toString();
                return ss.str();
            }
        }
        case SubType::kDataStop:
            return "BALANCE DATA STOP";
        case SubType::kDataReset:
            return "BALANCE DATA RESET PLAN";
        case SubType::kShowBalancePlan: {
            std::stringstream ss;
            ss << "BALANCE DATA " << balanceId_;
            return ss.str();
        }
    }
    DLOG(FATAL) << "Type illegal";
    return "Unknown";
}

std::string HostList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &host : hosts_) {
        buf += "\"";
        buf += host->host;
        buf += "\"";
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
    std::string buf;
    buf.reserve(64);
    buf += "ADD LISTENER ";
    switch (type_) {
        case meta::cpp2::ListenerType::ELASTICSEARCH:
            buf += "ELASTICSEARCH ";
            break;
        case meta::cpp2::ListenerType::UNKNOWN:
            LOG(FATAL) << "Unknown listener type.";
            break;
    }
    buf += listeners_->toString();
    return buf;
}

std::string RemoveListenerSentence::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "REMOVE LISTENER ";
    switch (type_) {
        case meta::cpp2::ListenerType::ELASTICSEARCH:
            buf += "ELASTICSEARCH ";
            break;
        case meta::cpp2::ListenerType::UNKNOWN:
            DLOG(FATAL) << "Unknown listener type.";
            break;
    }
    return buf;
}

std::string ShowListenerSentence::toString() const {
    return "SHOW LISTENER";
}

std::string AdminJobSentence::toString() const {
    switch (op_) {
        case meta::cpp2::AdminJobOp::ADD: {
            switch (cmd_) {
                case meta::cpp2::AdminCmd::COMPACT:
                    return paras_.empty() ? "SUBMIT JOB COMPACT"
                        : folly::stringPrintf("SUBMIT JOB COMPACT %s", paras_[0].c_str());
                case meta::cpp2::AdminCmd::FLUSH:
                    return paras_.empty() ? "SUBMIT JOB FLUSH"
                        : folly::stringPrintf("SUBMIT JOB FLUSH %s", paras_[0].c_str());
                case meta::cpp2::AdminCmd::REBUILD_TAG_INDEX:
                    return folly::stringPrintf("REBUILD TAG INDEX %s",
                            folly::join(",", paras_).c_str());
                case meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
                    return folly::stringPrintf("REBUILD EDGE INDEX %s",
                            folly::join(",", paras_).c_str());
                case meta::cpp2::AdminCmd::REBUILD_FULLTEXT_INDEX:
                    return "REBUILD FULLTEXT INDEX";
                case meta::cpp2::AdminCmd::STATS:
                    return paras_.empty() ? "SUBMIT JOB STATS"
                        : folly::stringPrintf("SUBMIT JOB STATS %s", paras_[0].c_str());
                case meta::cpp2::AdminCmd::DOWNLOAD:
                    return paras_.empty() ? "DOWNLOAD HDFS "
                        : folly::stringPrintf("DOWNLOAD HDFS %s", paras_[0].c_str());
                case meta::cpp2::AdminCmd::INGEST:
                    return "INGEST";
                case meta::cpp2::AdminCmd::DATA_BALANCE:
                case meta::cpp2::AdminCmd::UNKNOWN:
                    return folly::stringPrintf("Unsupported AdminCmd: %s",
                            apache::thrift::util::enumNameSafe(cmd_).c_str());
            }
        }
        case meta::cpp2::AdminJobOp::SHOW_All:
            return "SHOW JOBS";
        case meta::cpp2::AdminJobOp::SHOW:
            CHECK_EQ(paras_.size(), 1U);
            return folly::stringPrintf("SHOW JOB %s", paras_[0].c_str());
        case meta::cpp2::AdminJobOp::STOP:
            CHECK_EQ(paras_.size(), 1U);
            return folly::stringPrintf("STOP JOB %s", paras_[0].c_str());
        case meta::cpp2::AdminJobOp::RECOVER:
            return "RECOVER JOB";
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

void AdminJobSentence::addPara(const NameLabelList& paras) {
    const auto& labels = paras.labels();
    std::for_each(labels.begin(), labels.end(), [this](const auto& para) {
        paras_.emplace_back(*para);
    });
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
        if (client.user_ref().has_value() && !(*client.user_ref()).empty()) {
            buf += ", \"";
            buf += *client.get_user();
            buf += "\"";
        }
        if (client.pwd_ref().has_value() && !(*client.pwd_ref()).empty()) {
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

std::string ShowSessionsSentence::toString() const {
    if (isSetSessionID()) {
        return folly::stringPrintf("SHOW SESSION %ld", sessionId_);
    }
    return "SHOW SESSIONS";
}

std::string ShowQueriesSentence::toString() const {
    std::string buf = "SHOW";
    if (isAll()) {
        buf += " ALL";
    }
    buf += " QUERIES";
    return buf;
}

std::string KillQuerySentence::toString() const {
    std::string buf = "KILL QUERY (";
    if (isSetSession()) {
        buf += "session=";
        buf += sessionId()->toString();
        buf += ", ";
    }
    buf += "plan=";
    buf += epId()->toString();
    buf += ")";
    return buf;
}
}   // namespace nebula
