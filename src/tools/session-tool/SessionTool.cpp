/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <rocksdb/db.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/fs/FileUtils.h"
#include "common/time/TimeUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/ActiveHostsMan.h"

DEFINE_string(path, "", "rocksdb instance path");
DEFINE_string(operator, "", "type of operator, including list, count, groupCount and delete");
DEFINE_string(user, "", "user name ");
DEFINE_int32(limit, -1, "limit number ");

namespace nebula {
namespace meta {

class SessionTool {
 public:
  static Status sessionsCount(const char* path,
                              const std::string op,
                              const std::string user,
                              const int limit) {
    LOG(INFO) << "open rocksdb on " << path;
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    auto status = rocksdb::DB::OpenForReadOnly(options, path, &db);
    if (!status.ok()) {
      return Status::Error("Open rocksdb failed: %s", status.ToString().c_str());
    }
    rocksdb::ReadOptions roptions;
    rocksdb::Iterator* iter = db->NewIterator(roptions);
    if (!iter) {
      return Status::Error("Init iterator failed");
    }

    std::string prefix = "__sessions__";
    iter->Seek(rocksdb::Slice(prefix));
    {
      if ("count" == op) {
        int count = 0;
        while (iter->Valid() && iter->key().starts_with(prefix)) {
          if (!user.empty()) {
            auto session = MetaKeyUtils::parseSessionVal(
                folly::StringPiece(iter->value().data(), iter->value().size()));
            std::string userName = session.get_user_name();
            if (userName == user) {
              count++;
            }
          } else {
            count++;
          }
          iter->Next();
        }
        LOG(INFO) << "The total number of sessions is " << count;
      }
    }

    {
      if ("list" == op) {
        int count = 0;
        LOG(INFO) << "SessionId\tUserName\tSpaceName\tCreateTime\tUpdateTime\tGraphAddr\tClientIp";
        while (iter->Valid() && iter->key().starts_with(prefix)) {
          auto sessionID = MetaKeyUtils::getSessionId(
              folly::StringPiece(iter->key().data(), iter->key().size()));
          auto session = MetaKeyUtils::parseSessionVal(
              folly::StringPiece(iter->value().data(), iter->value().size()));
          std::string userName = session.get_user_name();
          if (!user.empty()) {
            if (userName == user) {
              LOG(INFO) << sessionID << "\t" << userName << "\t" << session.get_space_name() << "\t"
                        << session.get_create_time() << "\t" << session.get_update_time() << "\t"
                        << session.get_graph_addr() << "\t" << session.get_client_ip();
              count++;
            }
          } else {
            LOG(INFO) << sessionID << "\t" << userName << "\t" << session.get_space_name() << "\t"
                      << session.get_create_time() << "\t" << session.get_update_time() << "\t"
                      << session.get_graph_addr() << "\t" << session.get_client_ip();
            count++;
          }
          if (limit > 0 && count >= limit) {
            break;
          }
          iter->Next();
        }
      }
    }

    {
      if ("groupCount" == op) {
        auto map = std::unordered_map<std::string, int>();
        while (iter->Valid() && iter->key().starts_with(prefix)) {
          auto session = MetaKeyUtils::parseSessionVal(
              folly::StringPiece(iter->value().data(), iter->value().size()));
          std::string userName = session.get_user_name();
          auto findPtr = map.find(userName);
          if (findPtr == map.end()) {
            map.emplace(userName, 1);
          } else {
            findPtr->second += 1;
          }
          iter->Next();
        }

        LOG(INFO) << "## user\tsessionCount ##";
        auto start = map.begin();
        auto stop = map.end();
        while (start != stop) {
          LOG(INFO) << "# " << start->first << "\t" << start->second;
          start++;
        }
      }
    }

    if (iter) {
      delete iter;
    }

    if (db) {
      db->Close();
      delete db;
    }
    return Status::OK();
  }

  static Status deleteSession(const char* path, const std::string user) {
    LOG(INFO) << "open rocksdb on " << path;
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    auto status = rocksdb::DB::Open(options, path, &db);
    if (!status.ok()) {
      return Status::Error("Open rocksdb failed: %s", status.ToString().c_str());
    }
    rocksdb::ReadOptions roptions;
    rocksdb::Iterator* iter = db->NewIterator(roptions);
    if (!iter) {
      return Status::Error("Init iterator failed");
    }
    {
      std::string prefix = "__sessions__";
      if (user.empty()) {
        std::string prefix1 = "__sessiont__";
        LOG(INFO) << "Will delete all sessions of cluster ";
        db->DeleteRange(rocksdb::WriteOptions(),
                        db->DefaultColumnFamily(),
                        rocksdb::Slice(prefix),
                        rocksdb::Slice(prefix1));
        LOG(INFO) << "All Sessions have been deleted";
      } else {
        int count = 0;
        iter->Seek(rocksdb::Slice(prefix));
        LOG(INFO) << "Will delete all sessions of " << user << " user";
        while (iter->Valid() && iter->key().starts_with(prefix)) {
          auto session = MetaKeyUtils::parseSessionVal(
              folly::StringPiece(iter->value().data(), iter->value().size()));
          std::string userName = session.get_user_name();
          if (userName == user) {
            db->Delete(rocksdb::WriteOptions(), iter->key());
            count++;
          }
          iter->Next();
        }
        LOG(INFO) << count << " sessions of " << user << " user have been deleted";
      }
    }
    if (iter) {
      delete iter;
    }

    if (db) {
      db->Close();
      delete db;
    }
    return Status::OK();
  }

  static std::string getUsage() {
    std::string usage = "Usage: session_tool <path> <operator> [user] [limit] \n";
    usage += "options introduction: \n";
    usage +=
        "-path    [require] the meta rocksdb path, absolute path need to end with "
        "path/nebula/0/data\n";
    usage += "-operator    [require] operator type, including count、delete、list and groupCount\n";
    usage +=
        "-user    use name, it can be used with list/delete operator to get/delete sessions of the "
        "specificed user \n";
    usage += "-limit   show number of sessions, it can be used with 'list' operator\n";
    return usage;
  }
};

}  // namespace meta
}  // namespace nebula

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  google::SetUsageMessage(nebula::meta::SessionTool::getUsage());
  LOG(INFO) << "path: " << FLAGS_path;
  LOG(INFO) << "operator: " << FLAGS_operator;
  LOG(INFO) << "user: " << FLAGS_user;
  LOG(INFO) << "limit: " << FLAGS_limit;
  if (FLAGS_path.empty() ||
      nebula::fs::FileUtils::fileType(FLAGS_path.c_str()) == nebula::fs::FileType::NOTEXIST) {
    LOG(INFO) << "Please specify the meta rocksdb path, absolute path need to "
                 "end with "
                 "path/nebula/0/data";
    std::cout << nebula::meta::SessionTool::getUsage() << std::endl;
    return -1;
  }
  if (FLAGS_operator.empty() || ("count" != FLAGS_operator && "delete" != FLAGS_operator &&
                                 "groupCount" != FLAGS_operator && "list" != FLAGS_operator)) {
    LOG(INFO)
        << "Please specify the operator type, operator type includes count/delete/list/groupCount";
    std::cout << nebula::meta::SessionTool::getUsage() << std::endl;
    return -1;
  }
  auto status = nebula::Status::OK();
  if ("count" == FLAGS_operator || "list" == FLAGS_operator || "groupCount" == FLAGS_operator) {
    status = nebula::meta::SessionTool::sessionsCount(
        FLAGS_path.c_str(), FLAGS_operator, FLAGS_user, FLAGS_limit);
  } else {
    status = nebula::meta::SessionTool::deleteSession(FLAGS_path.c_str(), FLAGS_user);
  }
  if (!status.ok()) {
    LOG(ERROR) << status.toString();
  }
  return 0;
}
