/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/datatypes/HostAddr.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

class MetaServiceUtils final {
 public:
  MetaServiceUtils() = delete;

  static nebula::cpp2::ErrorCode alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                                 cpp2::SchemaProp& prop,
                                                 const cpp2::ColumnDef col,
                                                 const cpp2::AlterSchemaOp op,
                                                 bool isEdge = false);

  static nebula::cpp2::ErrorCode alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                                 cpp2::SchemaProp& schemaProp,
                                                 cpp2::SchemaProp alterSchemaProp,
                                                 bool existIndex,
                                                 bool isEdge = false);

  // backup
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupIndex(
      kvstore::KVStore* kvstore,
      const std::unordered_set<GraphSpaceID>& spaces,
      const std::string& backupName,
      const std::vector<std::string>* spaceName);

  static std::function<bool(const folly::StringPiece& key)> spaceFilter(
      const std::unordered_set<GraphSpaceID>& spaces,
      std::function<GraphSpaceID(folly::StringPiece rawKey)> parseSpace);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTables(
      kvstore::KVStore* kvstore,
      const std::unordered_set<GraphSpaceID>& spaceIds,
      const std::string& backupName,
      const std::vector<std::string>* spaceNames);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAUTILS_H_
