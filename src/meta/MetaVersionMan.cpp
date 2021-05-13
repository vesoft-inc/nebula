/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaVersionMan.h"
#include "meta/upgradeData/MetaDataUpgrade.h"
#include "meta/upgradeData/oldThrift/MetaServiceUtilsV1.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobDescription.h"

DEFINE_bool(null_type, true, "set schema to support null type");
DEFINE_bool(print_info, false, "enable to print the rewrite data");
DEFINE_uint32(string_index_limit, 64, "string index key length limit");

namespace nebula {
namespace meta {

static const std::string kMetaVersionKey = "__meta_version__";          // NOLINT

// static
MetaVersion MetaVersionMan::getMetaVersionFromKV(kvstore::KVStore* kv) {
    CHECK_NOTNULL(kv);
    std::string value;
    auto code = kv->get(kDefaultSpaceId, kDefaultPartId, kMetaVersionKey, &value, true);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        auto version = *reinterpret_cast<const MetaVersion*>(value.data());
        return (version == MetaVersion::V2) ? MetaVersion::V2 : MetaVersion::UNKNOWN;
    } else {
        return getVersionByHost(kv);
    }
}

// static
MetaVersion MetaVersionMan::getVersionByHost(kvstore::KVStore* kv) {
    const auto& hostPrefix = nebula::meta::MetaServiceUtils::hostPrefix();
    std::unique_ptr<nebula::kvstore::KVIterator> iter;
    auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter, true);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return MetaVersion::UNKNOWN;
    }
    if (iter->valid()) {
        auto v1KeySize = hostPrefix.size() + sizeof(int64_t);
        return (iter->key().size() == v1KeySize) ? MetaVersion::V1 : MetaVersion::V2;
    }
    // No hosts exists, regard as regard as version 2
    return MetaVersion::V2;
}

// static
bool MetaVersionMan::setMetaVersionToKV(kvstore::KVStore* kv) {
    CHECK_NOTNULL(kv);
    auto v2 = MetaVersion::V2;
    std::vector<kvstore::KV> data;
    data.emplace_back(kMetaVersionKey,
                      std::string(reinterpret_cast<const char*>(&v2), sizeof(MetaVersion)));
    bool ret = true;
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                      [&](nebula::cpp2::ErrorCode code) {
                          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                              LOG(ERROR) << "Put failed, error: " << static_cast<int32_t>(code);
                              ret = false;
                          } else {
                              LOG(INFO) << "Write meta version 2 succeeds";
                          }
                          baton.post();
                      });
    baton.wait();
    return ret;
}

// static
Status MetaVersionMan::updateMetaV1ToV2(kvstore::KVStore* kv) {
    CHECK_NOTNULL(kv);
    auto snapshot = folly::format("META_UPGRADE_SNAPSHOT_{}",
            MetaServiceUtils::genTimestampStr()).str();
    auto meteRet = kv->createCheckpoint(kDefaultSpaceId, snapshot);
    if (meteRet.isLeftType()) {
        LOG(ERROR) << "Create snapshot failed: " << snapshot;
        return Status::Error("Create snapshot failed");
    }
    auto status = doUpgrade(kv);
    if (!status.ok()) {
        // rollback by snapshot
        return status;
    }
    // delete snapshot file
    auto dmRet = kv->dropCheckpoint(kDefaultSpaceId, snapshot);
    if (dmRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Delete snapshot: " << snapshot
                   << " failed, You need to delete it manually";
    }
    return Status::OK();
}

// static
Status MetaVersionMan::doUpgrade(kvstore::KVStore* kv) {
    MetaDataUpgrade upgrader(kv);
    {
        // kSpacesTable
        auto prefix = nebula::oldmeta::kSpacesTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printSpaces(iter->val());
                }
                status = upgrader.rewriteSpaces(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kPartsTable
        auto prefix = nebula::oldmeta::kPartsTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printParts(iter->key(), iter->val());
                }
                status = upgrader.rewriteParts(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kHostsTable
        auto prefix = nebula::oldmeta::kHostsTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printHost(iter->key(), iter->val());
                }
                status = upgrader.rewriteHosts(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }
    {
        // kLeadersTable
        auto prefix = nebula::oldmeta::kLeadersTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printLeaders(iter->key());
                }
                status = upgrader.rewriteLeaders(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }
    {
        // kTagsTable
        auto prefix = nebula::oldmeta::kTagsTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printSchemas(iter->val());
                }
                status = upgrader.rewriteSchemas(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kEdgesTable
        auto prefix = nebula::oldmeta::kEdgesTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printSchemas(iter->val());
                }
                status = upgrader.rewriteSchemas(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kIndexesTable
        auto prefix = nebula::oldmeta::kIndexesTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printIndexes(iter->val());
                }
                status = upgrader.rewriteIndexes(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kConfigsTable
        auto prefix = nebula::oldmeta::kConfigsTable;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (FLAGS_print_info) {
                    upgrader.printConfigs(iter->key(), iter->val());
                }
                status = upgrader.rewriteConfigs(iter->key(), iter->val());
                if (!status.ok()) {
                    LOG(ERROR) << status;
                    return status;
                }
                iter->next();
            }
        }
    }

    {
        // kJob
        auto prefix = JobUtil::jobPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            Status status = Status::OK();
            while (iter->valid()) {
                if (JobDescription::isJobKey(iter->key())) {
                    if (FLAGS_print_info) {
                        upgrader.printJobDesc(iter->key(), iter->val());
                    }
                    status = upgrader.rewriteJobDesc(iter->key(), iter->val());
                    if (!status.ok()) {
                        LOG(ERROR) << status;
                        return status;
                    }
                } else {
                    // the job key format is change, need to delete the old format
                    status = upgrader.deleteKeyVal(iter->key());
                    if (!status.ok()) {
                        LOG(ERROR) << status;
                        return status;
                    }
                }
                iter->next();
            }
        }
    }

    // delete
    {
        std::vector<std::string> prefixes({nebula::oldmeta::kIndexStatusTable,
                                           nebula::oldmeta::kDefaultTable,
                                           nebula::oldmeta::kCurrJob,
                                           nebula::oldmeta::kJobArchive});
        std::unique_ptr <kvstore::KVIterator> iter;
        for (auto &prefix : prefixes) {
            auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
            if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
                Status status = Status::OK();
                while (iter->valid()) {
                    if (prefix == nebula::oldmeta::kIndexesTable) {
                        if (FLAGS_print_info) {
                            upgrader.printIndexes(iter->val());
                        }
                        auto oldItem = oldmeta::MetaServiceUtilsV1::parseIndex(iter->val());
                        auto spaceId = MetaServiceUtils::parseIndexesKeySpaceID(iter->key());
                        status = upgrader.deleteKeyVal(
                                MetaServiceUtils::indexIndexKey(spaceId, oldItem.get_index_name()));
                        if (!status.ok()) {
                            LOG(ERROR) << status;
                            return status;
                        }
                    }
                    status = upgrader.deleteKeyVal(iter->key());
                    if (!status.ok()) {
                        LOG(ERROR) << status;
                        return status;
                    }
                    iter->next();
                }
            }
        }
    }
    if (!setMetaVersionToKV(kv)) {
        return Status::Error("Persist meta version failed");
    } else {
        return Status::OK();
    }
}

}  // namespace meta
}  // namespace nebula

