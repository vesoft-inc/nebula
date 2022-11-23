/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/AdminSentences.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include <sstream>

#include "graph/util/SchemaUtil.h"

namespace nebula {

std::string ShowHostsSentence::toString() const {
  return std::string("SHOW HOSTS");
}

std::string ShowMetaLeaderSentence::toString() const {
  return std::string("SHOW META LEADER");
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

std::string DescribeUserSentence::toString() const {
  return folly::stringPrintf("DESCRIBE USER %s", account_.get()->c_str());
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
    case OptionType::PARTITION_NUM:
      return folly::stringPrintf("partition_num = %ld", std::get<int64_t>(optValue_));
    case OptionType::REPLICA_FACTOR:
      return folly::stringPrintf("replica_factor = %ld", std::get<int64_t>(optValue_));
    case OptionType::VID_TYPE: {
      auto &typeDef = std::get<meta::cpp2::ColumnTypeDef>(optValue_);
      return folly::stringPrintf("vid_type = %s", graph::SchemaUtil::typeToString(typeDef).c_str());
    }
    case OptionType::CHARSET:
      return folly::stringPrintf("charset = %s", std::get<std::string>(optValue_).c_str());
    case OptionType::COLLATE:
      return folly::stringPrintf("collate = %s", std::get<std::string>(optValue_).c_str());
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
  if (zoneNames_ != nullptr) {
    buf += " ON ";
    buf += zoneNames_->toString();
  }
  if (comment_ != nullptr) {
    buf += " comment = \"";
    buf += *comment_;
    buf += "\"";
  }
  return buf;
}

std::string CreateSpaceAsSentence::toString() const {
  auto buf = folly::sformat("CREATE SPACE {} AS {}", *newSpaceName_, *oldSpaceName_);
  return buf;
}

std::string DropSpaceSentence::toString() const {
  return folly::stringPrintf("DROP SPACE %s", spaceName_.get()->c_str());
}

std::string ClearSpaceSentence::toString() const {
  return folly::stringPrintf("CLEAR SPACE %s", spaceName_.get()->c_str());
}

std::string AlterSpaceSentence::toString() const {
  std::string zones = paras_.front();
  for (size_t i = 1; i < paras_.size(); i++) {
    zones += "," + paras_[i];
  }
  return folly::stringPrintf(
      "ALTER SPACE %s ADD ZONE %s", spaceName_.get()->c_str(), zones.c_str());
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
    case meta::cpp2::JobOp::ADD: {
      switch (type_) {
        case meta::cpp2::JobType::COMPACT:
          return "SUBMIT JOB COMPACT";
        case meta::cpp2::JobType::FLUSH:
          return "SUBMIT JOB FLUSH";
        case meta::cpp2::JobType::REBUILD_TAG_INDEX:
          return folly::stringPrintf("REBUILD TAG INDEX %s", folly::join(",", paras_).c_str());
        case meta::cpp2::JobType::REBUILD_EDGE_INDEX:
          return folly::stringPrintf("REBUILD EDGE INDEX %s", folly::join(",", paras_).c_str());
        case meta::cpp2::JobType::REBUILD_FULLTEXT_INDEX:
          return "REBUILD FULLTEXT INDEX";
        case meta::cpp2::JobType::STATS:
          return "SUBMIT JOB STATS";
        case meta::cpp2::JobType::DOWNLOAD:
          return folly::stringPrintf("SUBMIT JOB DOWNLOAD HDFS \"%s\"", paras_[0].c_str());
        case meta::cpp2::JobType::INGEST:
          return "SUBMIT JOB INGEST";
        case meta::cpp2::JobType::DATA_BALANCE:
          if (paras_.empty()) {
            return "SUBMIT JOB BALANCE IN ZONE";
          } else {
            std::string str = "SUBMIT JOB BALANCE IN ZONE REMOVE";
            for (size_t i = 0; i < paras_.size(); i++) {
              auto &s = paras_[i];
              str += i == 0 ? " " + s : ", " + s;
            }
            return str;
          }
        case meta::cpp2::JobType::ZONE_BALANCE:
          if (paras_.empty()) {
            return "SUBMIT JOB BALANCE ACROSS ZONE";
          } else {
            std::string str = "SUBMIT JOB BALANCE ACROSS ZONE REMOVE";
            for (size_t i = 0; i < paras_.size(); i++) {
              auto &s = paras_[i];
              str += i == 0 ? " " + s : ", " + s;
            }
            return str;
          }
        case meta::cpp2::JobType::LEADER_BALANCE:
          return "SUBMIT JOB BALANCE LEADER";
        case meta::cpp2::JobType::UNKNOWN:
          return folly::stringPrintf("Unsupported JobType: %s",
                                     apache::thrift::util::enumNameSafe(type_).c_str());
      }
    }
    case meta::cpp2::JobOp::SHOW_All:
      return "SHOW JOBS";
    case meta::cpp2::JobOp::SHOW:
      CHECK_EQ(paras_.size(), 1U);
      return folly::stringPrintf("SHOW JOB %s", paras_[0].c_str());
    case meta::cpp2::JobOp::STOP:
      CHECK_EQ(paras_.size(), 1U);
      return folly::stringPrintf("STOP JOB %s", paras_[0].c_str());
    case meta::cpp2::JobOp::RECOVER:
      if (paras_.empty()) {
        return "RECOVER JOB";
      } else {
        std::string str = "RECOVER JOB";
        for (size_t i = 0; i < paras_.size(); i++) {
          str += i == 0 ? " " + paras_[i] : ", " + paras_[i];
        }
        return str;
      }
  }
  LOG(FATAL) << "Unknown job operation " << static_cast<uint8_t>(op_);
}

meta::cpp2::JobOp AdminJobSentence::getOp() const {
  return op_;
}

meta::cpp2::JobType AdminJobSentence::getJobType() const {
  return type_;
}

const std::vector<std::string> &AdminJobSentence::getParas() const {
  return paras_;
}

void AdminJobSentence::addPara(const std::string &para) {
  paras_.emplace_back(para);
}

void AdminJobSentence::addPara(const NameLabelList &paras) {
  const auto &labels = paras.labels();
  std::for_each(
      labels.begin(), labels.end(), [this](const auto &para) { paras_.emplace_back(*para); });
}

std::string ShowStatsSentence::toString() const {
  return folly::stringPrintf("SHOW STATS");
}

std::string ShowServiceClientsSentence::toString() const {
  switch (type_) {
    case meta::cpp2::ExternalServiceType::ELASTICSEARCH:
      return "SHOW TEXT SEARCH CLIENTS";
    default:
      LOG(FATAL) << "Unknown service type " << static_cast<uint8_t>(type_);
  }
}

std::string SignInServiceSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  switch (type_) {
    case meta::cpp2::ExternalServiceType::ELASTICSEARCH:
      buf += "SIGN IN TEXT SERVICE ";
      break;
    default:
      LOG(FATAL) << "Unknown service type " << static_cast<uint8_t>(type_);
  }

  for (auto &client : clients_->clients()) {
    buf += "(";
    buf += client.get_host().host;
    buf += ":";
    buf += std::to_string(client.get_host().port);
    if (client.conn_type_ref().has_value()) {
      std::string connType = *client.get_conn_type();
      auto toupper = [](auto c) { return ::toupper(c); };
      std::transform(connType.begin(), connType.end(), connType.begin(), toupper);
      buf += ", ";
      buf += connType;
    }
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
    buf += ", ";
  }
  if (!buf.empty()) {
    buf.resize(buf.size() - 2);
  }
  return buf;
}

std::string SignOutServiceSentence::toString() const {
  switch (type_) {
    case meta::cpp2::ExternalServiceType::ELASTICSEARCH:
      return "SIGN OUT TEXT SERVICE";
    default:
      LOG(FATAL) << "Unknown service type " << static_cast<uint8_t>(type_);
  }
}

std::string ShowSessionsSentence::toString() const {
  if (isSetSessionID()) {
    return folly::stringPrintf("SHOW SESSION %ld", sessionId_);
  }
  if (isLocalCommand()) return "SHOW LOCAL SESSIONS";
  return "SHOW SESSIONS";
}

std::string ShowQueriesSentence::toString() const {
  std::string buf = "SHOW";
  if (!isAll()) {
    buf += " LOCAL";
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
}  // namespace nebula
