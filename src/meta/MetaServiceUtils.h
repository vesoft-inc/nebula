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

  /**
   * @brief Change column definition in memory
   *
   * @param cols Existed columns in schema
   * @param prop Schema property, mainly used to check if the col is ttl_col
   * @param col The column that will be operated
   * @param op
   * ADD: add col to cols
   * CHANGE: replace the column in cols with col
   * DROP: remove the col from cols
   * @param isEdge Is edge or tag
   * @return
   */
  static nebula::cpp2::ErrorCode alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                                 cpp2::SchemaProp& prop,
                                                 const cpp2::ColumnDef col,
                                                 const cpp2::AlterSchemaOp op,
                                                 bool isEdge = false);

  /**
   * @brief Change schema property, mainly set ttl_col
   *
   * @param cols Column infomartion, mainly used to check if the colType is INT64 or TIMESTAMP
   * @param schemaProp Which schema property to change
   * @param alterSchemaProp Where to get ttl_col
   * @param existIndex If the column has index
   * @param isEdge Edge or tag?
   * @return
   */
  static nebula::cpp2::ErrorCode alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                                 cpp2::SchemaProp& schemaProp,
                                                 cpp2::SchemaProp alterSchemaProp,
                                                 bool existIndex,
                                                 bool isEdge = false);

  /**
   * @brief Backup index
   *
   * @param kvstore Which kvStore to backup
   * @param spaces Some keys need to filter by space id
   * @param backupName Be used to make the name of backup file
   * @param spaceName Some keys need to filter by space name
   * @return Backup files
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupIndex(
      kvstore::KVStore* kvstore,
      const std::unordered_set<GraphSpaceID>& spaces,
      const std::string& backupName,
      const std::vector<std::string>* spaceName);

  /**
   * @brief Make a function that filter spaces
   *
   * @param spaces Be used to filter keys don't contained
   * @param parseSpace Funtion that parse the key to space id
   * @return
   */
  static std::function<bool(const folly::StringPiece& key)> spaceFilter(
      const std::unordered_set<GraphSpaceID>& spaces,
      std::function<GraphSpaceID(folly::StringPiece rawKey)> parseSpace);

  /**
   * @brief Backup tables, backup system tables if backup all spaces
   *
   * @param kvstore Which kvStore to backup
   * @param spaceIds Skip keys not belong to these spaces
   * @param backupName Be used to make the name of backup file
   * @param spaceNames Null means all spaces, also used in backup indexes
   * @return Backup files
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTables(
      kvstore::KVStore* kvstore,
      const std::unordered_set<GraphSpaceID>& spaceIds,
      const std::string& backupName,
      const std::vector<std::string>* spaceNames);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAUTILS_H_
