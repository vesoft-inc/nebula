/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <rocksdb/db.h>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "meta/upgrade/v1/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class MetaDataUpgrade final {
 public:
  explicit MetaDataUpgrade(kvstore::KVStore *kv) : kv_(kv) {}

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
  Status put(const folly::StringPiece &key, const folly::StringPiece &val);

  Status remove(const folly::StringPiece &key);

  Status convertToNewColumns(const std::vector<meta::v1::cpp2::ColumnDef> &oldCols,
                             std::vector<cpp2::ColumnDef> &newCols);

  Status convertToNewIndexColumns(const std::vector<meta::v1::cpp2::ColumnDef> &oldCols,
                                  std::vector<cpp2::ColumnDef> &newCols);

 private:
  kvstore::KVStore *kv_ = nullptr;
};

}  // namespace meta
}  // namespace nebula
