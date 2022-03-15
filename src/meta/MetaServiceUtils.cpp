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
}  // namespace

nebula::cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                                          cpp2::SchemaProp& prop,
                                                          const cpp2::ColumnDef col,
                                                          const cpp2::AlterSchemaOp op,
                                                          bool isEdge) {
  switch (op) {
    case cpp2::AlterSchemaOp::ADD:
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        if (it->get_name() == col.get_name()) {
          LOG(INFO) << "Column existing: " << col.get_name();
          return nebula::cpp2::ErrorCode::E_EXISTED;
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
