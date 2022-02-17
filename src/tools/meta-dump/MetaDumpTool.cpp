/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Format.h>                   // for sformat
#include <folly/Range.h>                    // for StringPiece
#include <folly/init/Init.h>                // for init
#include <gflags/gflags.h>                  // for clstring, DEFINE_string
#include <glog/logging.h>                   // for INFO
#include <rocksdb/db.h>                     // for DB
#include <rocksdb/iterator.h>               // for Iterator
#include <rocksdb/options.h>                // for Options, ReadOptions
#include <rocksdb/slice.h>                  // for Slice
#include <rocksdb/status.h>                 // for Status
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <string>   // for operator<<, string, all...
#include <tuple>    // for tie, tuple
#include <utility>  // for pair
#include <vector>   // for vector

#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG_INFO
#include "common/base/Status.h"               // for Status
#include "common/datatypes/Date.h"            // for DateTime
#include "common/datatypes/HostAddr.h"        // for operator<<, HostAddr
#include "common/fs/FileUtils.h"              // for FileType, FileType::NOT...
#include "common/thrift/ThriftTypes.h"        // for TermID
#include "common/time/TimeConversion.h"       // for TimeConversion
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "interface/gen-cpp2/meta_types.h"    // for SpaceDesc
#include "meta/ActiveHostsMan.h"              // for HostInfo

DEFINE_string(path, "", "rocksdb instance path");

namespace nebula {
namespace meta {

class MetaDumper {
 public:
  static Status dumpMeta(const char* path) {
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
    std::string prefix;
    {
      LOG(INFO) << "Space info";
      prefix = "__spaces__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());
        auto spaceId = MetaKeyUtils::spaceId(key);
        auto desc = MetaKeyUtils::parseSpace(val);
        LOG(INFO) << folly::sformat(
            "space id: {}, space name: {}, partition num: {}, replica_factor: "
            "{}",
            spaceId,
            *desc.space_name_ref(),
            *desc.partition_num_ref(),
            *desc.replica_factor_ref());
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Partition info";
      prefix = "__parts__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());
        auto spaceId = MetaKeyUtils::parsePartKeySpaceId(key);
        auto partId = MetaKeyUtils::parsePartKeyPartId(key);
        auto hosts = MetaKeyUtils::parsePartVal(val);
        std::stringstream ss;
        for (const auto& host : hosts) {
          ss << host << " ";
        }
        LOG(INFO) << folly::sformat(
            "space id: {}, part id: {}, hosts: {}", spaceId, partId, ss.str());
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Host info";
      prefix = "__hosts__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());
        auto host = MetaKeyUtils::parseHostKey(key);
        auto info = HostInfo::decode(val);
        auto role = apache::thrift::util::enumNameSafe(info.role_);
        auto time = time::TimeConversion::unixSecondsToDateTime(info.lastHBTimeInMilliSec_ / 1000);
        LOG(INFO) << folly::sformat(
            "host addr: {}, role: {}, last hb time: {}", host.toString(), role, time.toString());
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Tag info";
      prefix = "__tags__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto spaceId = MetaKeyUtils::parseTagsKeySpaceID(key);
        auto tagId = MetaKeyUtils::parseTagId(key);
        auto ver = MetaKeyUtils::parseTagVersion(key);
        LOG(INFO) << folly::sformat("space id: {}, tag id: {}, ver: {}", spaceId, tagId, ver);
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Edge info";
      prefix = "__edges__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto spaceId = MetaKeyUtils::parseEdgesKeySpaceID(key);
        auto type = MetaKeyUtils::parseEdgeType(key);
        auto ver = MetaKeyUtils::parseEdgeVersion(key);
        LOG(INFO) << folly::sformat("space id: {}, edge type: {}, ver: {}", spaceId, type, ver);
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Index info";
      prefix = "__indexes__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto spaceId = MetaKeyUtils::parseIndexesKeySpaceID(key);
        auto indexId = MetaKeyUtils::parseIndexesKeyIndexID(key);
        LOG(INFO) << folly::sformat("space id: {}, index: {}", spaceId, indexId);
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Leader info";
      prefix = "__leader_terms__";
      HostAddr host;
      TermID term;
      nebula::cpp2::ErrorCode code;
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());
        auto spaceIdAndPartId = MetaKeyUtils::parseLeaderKeyV3(key);

        std::tie(host, term, code) = MetaKeyUtils::parseLeaderValV3(val);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          LOG(ERROR) << folly::sformat("leader space id: {}, part id: {} illegal.",
                                       spaceIdAndPartId.first,
                                       spaceIdAndPartId.second);
        } else {
          LOG(INFO) << folly::sformat("leader space id: {}, part id: {}, host: {}, term: {}",
                                      spaceIdAndPartId.first,
                                      spaceIdAndPartId.second,
                                      host.toString(),
                                      term);
        }
        iter->Next();
      }
    }
    {
      LOG(INFO) << "Zone info";
      prefix = "__zones__";
      iter->Seek(rocksdb::Slice(prefix));
      while (iter->Valid() && iter->key().starts_with(prefix)) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());
        auto zone = MetaKeyUtils::parseZoneName(key);
        auto hosts = MetaKeyUtils::parseZoneHosts(val);
        std::stringstream ss;
        for (const auto& host : hosts) {
          ss << host << " ";
        }
        LOG(INFO) << folly::sformat("zone name: {}, contain hosts: {}", zone, ss.str());
        iter->Next();
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
};

}  // namespace meta
}  // namespace nebula

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  LOG(INFO) << "path: " << FLAGS_path;
  if (FLAGS_path.empty() ||
      nebula::fs::FileUtils::fileType(FLAGS_path.c_str()) == nebula::fs::FileType::NOTEXIST) {
    LOG(INFO) << "Please specify the meta rocksdb path, absolute path need to "
                 "end with "
                 "path/nebula/0/data";
    return -1;
  }
  nebula::meta::MetaDumper dumper;
  auto status = dumper.dumpMeta(FLAGS_path.c_str());
  if (!status.ok()) {
    LOG(ERROR) << status.toString();
  }
  return 0;
}
