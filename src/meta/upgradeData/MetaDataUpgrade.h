/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_METADATAUPGRADETOOL_METADATAUPGRADE_H_
#define TOOLS_METADATAUPGRADETOOL_METADATAUPGRADE_H_

#include <rocksdb/db.h>
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "meta/upgradeData/oldThrift/gen-cpp2/old_meta_types.h"


namespace nebula {
namespace meta {

class MetaDataUpgrade final {
public:
    explicit MetaDataUpgrade(kvstore::KVStore* kv) : kv_(kv) {}

    ~MetaDataUpgrade() = default;

    Status rewriteHosts(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteSpaces(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteParts(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteLeaders(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteSchemas(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteIndexes(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteConfigs(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteJobDesc(const folly::StringPiece &key, const folly::StringPiece &val);

    Status deleteKeyVal(const folly::StringPiece &key);

    void printHost(const folly::StringPiece &key, const folly::StringPiece &val);

    void printSpaces(const folly::StringPiece &val);

    void printParts(const folly::StringPiece &key, const folly::StringPiece &val);

    void printLeaders(const folly::StringPiece &key);

    void printSchemas(const folly::StringPiece &val);

    void printIndexes(const folly::StringPiece &val);

    void printConfigs(const folly::StringPiece &key, const folly::StringPiece &val);

    void printJobDesc(const folly::StringPiece &key, const folly::StringPiece &val);

private:
    Status put(const folly::StringPiece &key, const folly::StringPiece &val) {
        std::vector<kvstore::KV> data;
        data.emplace_back(key.str(), val.str());
        folly::Baton<true, std::atomic> baton;
        auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
        kv_->asyncMultiPut(kDefaultSpaceId,
                           kDefaultPartId,
                           std::move(data),
                           [&ret, &baton] (nebula::cpp2::ErrorCode code) {
                               if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                                   ret = code;
                                   LOG(INFO) << "Put data error on meta server";
                               }
                               baton.post();
                           });
        baton.wait();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return Status::Error("Put data failed");
        }
        return Status::OK();
    }

    Status remove(const folly::StringPiece& key) {
        std::vector<std::string> keys{key.str()};
        folly::Baton<true, std::atomic> baton;
        auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
        kv_->asyncMultiRemove(kDefaultSpaceId,
                              kDefaultPartId,
                              std::move(keys),
                              [&ret, &baton] (nebula::cpp2::ErrorCode code) {
                                  if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                                      ret = code;
                                      LOG(INFO) << "Remove data error on meta server";
                                  }
                                  baton.post();
                              });
        baton.wait();
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return Status::Error("Remove data failed");
        }
        return Status::OK();;
    }

    Status convertToNewColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                               std::vector<cpp2::ColumnDef> &newCols);

    Status convertToNewIndexColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                                    std::vector<cpp2::ColumnDef> &newCols);

private:
    kvstore::KVStore*        kv_ = nullptr;
};

}  // namespace meta
}  // namespace nebula

#endif  // TOOLS_METADATAUPGRADETOOL_METADATAUPGRADE_H_
