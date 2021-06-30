/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <rocksdb/db.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include "common/time/TimeUtils.h"
#include "common/fs/FileUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"

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
                auto spaceId = MetaServiceUtils::spaceId(key);
                auto desc = MetaServiceUtils::parseSpace(val);
                LOG(INFO) << folly::sformat(
                    "space id: {}, space name: {}, partition num: {}, replica_factor: {}",
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
                auto spaceId = MetaServiceUtils::parsePartKeySpaceId(key);
                auto partId = MetaServiceUtils::parsePartKeyPartId(key);
                auto hosts = MetaServiceUtils::parsePartVal(val);
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
                auto host = MetaServiceUtils::parseHostKey(key);
                auto info = HostInfo::decode(val);
                auto role = apache::thrift::util::enumNameSafe(info.role_);
                auto time =
                    time::TimeConversion::unixSecondsToDateTime(info.lastHBTimeInMilliSec_ / 1000);
                LOG(INFO) << folly::sformat("host addr: {}, role: {}, last hb time: {}",
                                            host.toString(),
                                            role,
                                            time.toString());
                iter->Next();
            }
        }
        {
            LOG(INFO) << "Tag info";
            prefix = "__tags__";
            iter->Seek(rocksdb::Slice(prefix));
            while (iter->Valid() && iter->key().starts_with(prefix)) {
                auto key = folly::StringPiece(iter->key().data(), iter->key().size());
                auto spaceId = MetaServiceUtils::parseTagsKeySpaceID(key);
                auto tagId = MetaServiceUtils::parseTagId(key);
                auto ver = MetaServiceUtils::parseTagVersion(key);
                LOG(INFO) << folly::sformat(
                    "space id: {}, tag id: {}, ver: {}", spaceId, tagId, ver);
                iter->Next();
            }
        }
        {
            LOG(INFO) << "Edge info";
            prefix = "__edges__";
            iter->Seek(rocksdb::Slice(prefix));
            while (iter->Valid() && iter->key().starts_with(prefix)) {
                auto key = folly::StringPiece(iter->key().data(), iter->key().size());
                auto spaceId = MetaServiceUtils::parseEdgesKeySpaceID(key);
                auto type = MetaServiceUtils::parseEdgeType(key);
                auto ver = MetaServiceUtils::parseEdgeVersion(key);
                LOG(INFO) << folly::sformat(
                    "space id: {}, edge type: {}, ver: {}", spaceId, type, ver);
                iter->Next();
            }
        }
        {
            LOG(INFO) << "Index info";
            prefix = "__indexes__";
            iter->Seek(rocksdb::Slice(prefix));
            while (iter->Valid() && iter->key().starts_with(prefix)) {
                auto key = folly::StringPiece(iter->key().data(), iter->key().size());
                auto spaceId = MetaServiceUtils::parseIndexesKeySpaceID(key);
                auto indexId = MetaServiceUtils::parseIndexesKeyIndexID(key);
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
                auto spaceIdAndPartId = MetaServiceUtils::parseLeaderKeyV3(key);

                std::tie(host, term, code) = MetaServiceUtils::parseLeaderValV3(val);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(ERROR) << folly::sformat("leader space id: {}, part id: {} illegal.",
                                                 spaceIdAndPartId.first,
                                                 spaceIdAndPartId.second);
                } else {
                    LOG(INFO) << folly::sformat(
                            "leader space id: {}, part id: {}, host: {}, term: {}",
                            spaceIdAndPartId.first,
                            spaceIdAndPartId.second,
                            host.toString(),
                            term);
                }
                iter->Next();
            }
        }
        {
            LOG(INFO) << "Group info";
            prefix = "__groups__";
            iter->Seek(rocksdb::Slice(prefix));
            while (iter->Valid() && iter->key().starts_with(prefix)) {
                auto key = folly::StringPiece(iter->key().data(), iter->key().size());
                auto val = folly::StringPiece(iter->value().data(), iter->value().size());
                auto group = MetaServiceUtils::parseGroupName(key);
                auto zones = MetaServiceUtils::parseZoneNames(val);
                std::stringstream ss;
                for (const auto& zone : zones) {
                    ss << zone << " ";
                }
                LOG(INFO) << folly::sformat("group name: {}, contain zones: {}", group, ss.str());
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
                auto zone = MetaServiceUtils::parseZoneName(key);
                auto hosts = MetaServiceUtils::parseZoneHosts(val);
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

}   // namespace meta
}   // namespace nebula

int main(int argc, char* argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    LOG(INFO) << "path: " << FLAGS_path;
    if (FLAGS_path.empty() ||
        nebula::fs::FileUtils::fileType(FLAGS_path.c_str()) == nebula::fs::FileType::NOTEXIST) {
        LOG(INFO) << "Please specify the meta rocksdb path, absolute path need to end with "
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
