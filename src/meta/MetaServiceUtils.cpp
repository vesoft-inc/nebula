/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaServiceUtils.h"

#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

namespace {
nebula::cpp2::ErrorCode backupTable(kvstore::KVStore* kvstore,
                                    const std::string& backupName,
                                    const std::string& tableName,
                                    std::vector<std::string>& files,
                                    std::function<bool(const folly::StringPiece& key)> filter) {
  auto backupRet = kvstore->backupTable(kDefaultSpaceId, backupName, tableName, filter);
  if (!ok(backupRet)) {
    auto code = error(backupRet);
    if (code == nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return code;
  }

  auto backupTableFiles = std::move(value(backupRet));
  files.insert(files.end(),
               std::make_move_iterator(backupTableFiles.begin()),
               std::make_move_iterator(backupTableFiles.end()));
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool isLegalTypeConversion(cpp2::ColumnTypeDef from, cpp2::ColumnTypeDef to) {
  // If not type change, return true. Fixed string will be handled separately.
  if (from.get_type() == to.get_type() &&
      from.get_type() != nebula::cpp2::PropertyType::FIXED_STRING) {
    return true;
  }
  // For unset type, always return true
  if (from.get_type() == nebula::cpp2::PropertyType::UNKNOWN) {
    return true;
  }
  // fixed string can be converted to string or wider fixed string
  if (from.get_type() == nebula::cpp2::PropertyType::FIXED_STRING) {
    if (to.get_type() == nebula::cpp2::PropertyType::STRING) {
      return true;
    } else if (to.get_type() == nebula::cpp2::PropertyType::FIXED_STRING) {
      if (!from.type_length_ref().has_value() || !to.type_length_ref().has_value()) {
        return false;
      }
      return *from.type_length_ref() <= *to.type_length_ref();
    } else {
      return false;
    }
  }
  // int is only allowed to convert to wider int
  if (from.get_type() == nebula::cpp2::PropertyType::INT32) {
    return to.get_type() == nebula::cpp2::PropertyType::INT64;
  }
  if (from.get_type() == nebula::cpp2::PropertyType::INT16) {
    return to.get_type() == nebula::cpp2::PropertyType::INT64 ||
           to.get_type() == nebula::cpp2::PropertyType::INT32;
  }
  if (from.get_type() == nebula::cpp2::PropertyType::INT8) {
    return to.get_type() == nebula::cpp2::PropertyType::INT64 ||
           to.get_type() == nebula::cpp2::PropertyType::INT32 ||
           to.get_type() == nebula::cpp2::PropertyType::INT16;
  }
  // Float is only allowed to convert to double
  if (from.get_type() == nebula::cpp2::PropertyType::FLOAT) {
    return to.get_type() == nebula::cpp2::PropertyType::DOUBLE;
  }
  // Forbid all the other conversion, as the old data are too different from the new data.
  return false;
}
}  // namespace

nebula::cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(
    std::vector<cpp2::ColumnDef>& cols,
    cpp2::SchemaProp& prop,
    const cpp2::ColumnDef col,
    const cpp2::AlterSchemaOp op,
    const std::vector<std::vector<cpp2::ColumnDef>>& allVersionedCols,
    bool isEdge) {
  switch (op) {
    case cpp2::AlterSchemaOp::ADD:
      // Check the current schema first. Then check all schemas.
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        if (it->get_name() == col.get_name()) {
          LOG(ERROR) << "Column existing: " << col.get_name();
          return nebula::cpp2::ErrorCode::E_EXISTED;
        }
      }
      // There won't any two columns having the same name across all schemas. If there is a column
      // having the same name with the intended change, it must be from history schemas.
      for (auto& versionedCols : allVersionedCols) {
        for (auto it = versionedCols.begin(); it != versionedCols.end(); ++it) {
          if (it->get_name() == col.get_name()) {
            LOG(ERROR) << "Column previously existing: " << col.get_name();
            return nebula::cpp2::ErrorCode::E_HISTORY_CONFLICT;
          }
        }
      }
      cols.emplace_back(std::move(col));
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    case cpp2::AlterSchemaOp::CHANGE:
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        auto colName = col.get_name();
        if (colName == it->get_name()) {
          // If this col is ttl_col, change not allowed
          if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
            LOG(INFO) << "Column: " << colName << " as ttl_col, change not allowed";
            return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
          }
          if (!isLegalTypeConversion(it->get_type(), col.get_type())) {
            LOG(ERROR) << "Update colume type " << colName << " from "
                       << apache::thrift::util::enumNameSafe(it->get_type().get_type()) << " to "
                       << apache::thrift::util::enumNameSafe(col.get_type().get_type())
                       << " is not allowed!";
            return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
          }
          *it = col;
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
      }
      LOG(INFO) << "Column not found: " << col.get_name();
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    case cpp2::AlterSchemaOp::DROP:
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        auto colName = col.get_name();
        if (colName == it->get_name()) {
          if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
            prop.ttl_duration_ref() = 0;
            prop.ttl_col_ref() = "";
          }
          cols.erase(it);
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
      }
      LOG(INFO) << "Column not found: " << col.get_name();
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    default:
      LOG(INFO) << "Alter schema operator not supported";
      return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
  }
}

nebula::cpp2::ErrorCode MetaServiceUtils::alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                                          cpp2::SchemaProp& schemaProp,
                                                          cpp2::SchemaProp alterSchemaProp,
                                                          bool existIndex,
                                                          bool isEdge) {
  if (existIndex && (alterSchemaProp.ttl_duration_ref().has_value() ||
                     alterSchemaProp.ttl_col_ref().has_value())) {
    LOG(INFO) << "Has index, can't change ttl";
    return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
  }
  if (alterSchemaProp.ttl_duration_ref().has_value()) {
    // Graph check  <=0 to = 0
    schemaProp.ttl_duration_ref() = *alterSchemaProp.ttl_duration_ref();
  }
  if (alterSchemaProp.ttl_col_ref().has_value()) {
    auto ttlCol = *alterSchemaProp.ttl_col_ref();
    // Disable ttl, ttl_col is empty, ttl_duration is 0
    if (ttlCol.empty()) {
      schemaProp.ttl_duration_ref() = 0;
      schemaProp.ttl_col_ref() = ttlCol;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    auto existed = false;
    for (auto& col : cols) {
      if (col.get_name() == ttlCol) {
        auto colType = col.get_type().get_type();
        // Only integer and timestamp columns can be used as ttl_col
        if (colType != nebula::cpp2::PropertyType::INT64 &&
            colType != nebula::cpp2::PropertyType::TIMESTAMP) {
          LOG(INFO) << "TTL column type illegal";
          return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
        }
        existed = true;
        schemaProp.ttl_col_ref() = ttlCol;
        break;
      }
    }

    if (!existed) {
      LOG(INFO) << "TTL column not found: " << ttlCol;
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    }
  }

  // Disable implicit TTL mode
  if ((schemaProp.get_ttl_duration() && (*schemaProp.get_ttl_duration() != 0)) &&
      (!schemaProp.get_ttl_col() ||
       (schemaProp.get_ttl_col() && schemaProp.get_ttl_col()->empty()))) {
    LOG(INFO) << "Implicit ttl_col not support";
    return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
  }

  if (alterSchemaProp.comment_ref().has_value()) {
    schemaProp.comment_ref() = *alterSchemaProp.comment_ref();
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::function<bool(const folly::StringPiece& key)> MetaServiceUtils::spaceFilter(
    const std::unordered_set<GraphSpaceID>& spaces,
    std::function<GraphSpaceID(folly::StringPiece rawKey)> parseSpace) {
  auto sf = [spaces, parseSpace](const folly::StringPiece& key) -> bool {
    if (spaces.empty()) {
      return false;
    }

    auto id = parseSpace(key);
    auto it = spaces.find(id);
    if (it == spaces.end()) {
      return true;
    }

    return false;
  };

  return sf;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> MetaServiceUtils::backupIndex(
    kvstore::KVStore* kvstore,
    const std::unordered_set<GraphSpaceID>& spaceIds,
    const std::string& backupName,
    const std::vector<std::string>* spaceNames) {
  auto indexTable = MetaKeyUtils::getIndexTable();
  return kvstore->backupTable(kDefaultSpaceId,
                              backupName,
                              indexTable,
                              // will filter out the index table when this function returns true
                              [spaceIds, spaceNames](const folly::StringPiece& key) -> bool {
                                if (spaceIds.empty()) {
                                  return false;
                                }

                                // space index: space name -> space id
                                auto type = MetaKeyUtils::parseIndexType(key);
                                if (type == EntryType::SPACE) {
                                  if (spaceNames == nullptr || spaceNames->empty()) {
                                    return false;
                                  }

                                  auto spaceName = MetaKeyUtils::parseIndexSpaceKey(key);
                                  LOG(INFO) << "Space name was " << spaceName;
                                  auto it = std::find_if(
                                      spaceNames->cbegin(),
                                      spaceNames->cend(),
                                      [&spaceName](auto& name) { return spaceName == name; });
                                  if (it == spaceNames->cend()) {
                                    return true;
                                  }
                                  return false;
                                }

                                // other index: space id -> values
                                auto id = MetaKeyUtils::parseIndexKeySpaceID(key);
                                auto it = spaceIds.find(id);
                                if (it == spaceIds.end()) {
                                  return true;
                                }
                                return false;
                              });
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> MetaServiceUtils::backupTables(
    kvstore::KVStore* kvstore,
    const std::unordered_set<GraphSpaceID>& spaceIds,
    const std::string& backupName,
    const std::vector<std::string>* spaceNames) {
  std::vector<std::string> files;

  // backup space relative tables
  auto tables = MetaKeyUtils::getTableMaps();
  for (const auto& table : tables) {
    if (table.second.second == nullptr) {
      LOG(INFO) << table.first << " table skipped";
      continue;
    }
    auto result = backupTable(
        kvstore, backupName, table.second.first, files, spaceFilter(spaceIds, table.second.second));
    if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return result;
    }
    LOG(INFO) << table.first << " table backup succeeded";
  }

  // backup system tables if backup all spaces
  bool allSpaces = spaceNames == nullptr || spaceNames->empty();
  if (allSpaces) {
    auto sysTables = MetaKeyUtils::getSystemTableMaps();
    for (const auto& table : sysTables) {
      if (!table.second.second) {
        LOG(INFO) << table.first << " table skipped";
        continue;
      }
      auto result = backupTable(kvstore, backupName, table.second.first, files, nullptr);
      if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return result;
      }
      LOG(INFO) << table.first << " table backup succeeded";
    }
  }

  // backup system info tables
  auto sysInfos = MetaKeyUtils::getSystemInfoMaps();
  for (const auto& table : sysInfos) {
    if (!table.second.second) {
      LOG(INFO) << table.first << " table skipped";
      continue;
    }
    auto result = backupTable(kvstore, backupName, table.second.first, files, nullptr);
    if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return result;
    }
    LOG(INFO) << table.first << " table backup succeeded";
  }

  // backup the mapping of space name and space id separately,
  // which skipped in space relative tables
  auto ret = backupIndex(kvstore, spaceIds, backupName, spaceNames);
  if (!ok(ret)) {
    auto result = error(ret);
    if (result == nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
      return files;
    }
    return result;
  }

  files.insert(files.end(),
               std::make_move_iterator(value(ret).begin()),
               std::make_move_iterator(value(ret).end()));

  return files;
}

}  // namespace meta
}  // namespace nebula
