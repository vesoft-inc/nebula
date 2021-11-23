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
  auto backupFilePath = kvstore->backupTable(kDefaultSpaceId, backupName, tableName, filter);
  if (!ok(backupFilePath)) {
    auto result = error(backupFilePath);
    if (result == nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return result;
  }

  files.insert(files.end(),
               std::make_move_iterator(value(backupFilePath).begin()),
               std::make_move_iterator(value(backupFilePath).end()));
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
          LOG(ERROR) << "Column existing: " << col.get_name();
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
            LOG(ERROR) << "Column: " << colName << " as ttl_col, change not allowed";
            return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
          }
          *it = col;
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
      }
      LOG(ERROR) << "Column not found: " << col.get_name();
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    case cpp2::AlterSchemaOp::DROP:
      for (auto it = cols.begin(); it != cols.end(); ++it) {
        auto colName = col.get_name();
        if (colName == it->get_name()) {
          if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
            prop.set_ttl_duration(0);
            prop.set_ttl_col("");
          }
          cols.erase(it);
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
      }
      LOG(ERROR) << "Column not found: " << col.get_name();
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    default:
      LOG(ERROR) << "Alter schema operator not supported";
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
    LOG(ERROR) << "Has index, can't change ttl";
    return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
  }
  if (alterSchemaProp.ttl_duration_ref().has_value()) {
    // Graph check  <=0 to = 0
    schemaProp.set_ttl_duration(*alterSchemaProp.ttl_duration_ref());
  }
  if (alterSchemaProp.ttl_col_ref().has_value()) {
    auto ttlCol = *alterSchemaProp.ttl_col_ref();
    // Disable ttl, ttl_col is empty, ttl_duration is 0
    if (ttlCol.empty()) {
      schemaProp.set_ttl_duration(0);
      schemaProp.set_ttl_col(ttlCol);
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    auto existed = false;
    for (auto& col : cols) {
      if (col.get_name() == ttlCol) {
        auto colType = col.get_type().get_type();
        // Only integer and timestamp columns can be used as ttl_col
        if (colType != nebula::cpp2::PropertyType::INT64 &&
            colType != nebula::cpp2::PropertyType::TIMESTAMP) {
          LOG(ERROR) << "TTL column type illegal";
          return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
        }
        existed = true;
        schemaProp.set_ttl_col(ttlCol);
        break;
      }
    }

    if (!existed) {
      LOG(ERROR) << "TTL column not found: " << ttlCol;
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
    LOG(WARNING) << "Implicit ttl_col not support";
    return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
  }

  if (alterSchemaProp.comment_ref().has_value()) {
    schemaProp.set_comment(*alterSchemaProp.comment_ref());
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
    const std::unordered_set<GraphSpaceID>& spaces,
    const std::string& backupName,
    const std::vector<std::string>* spaceName) {
  auto indexTable = MetaKeyUtils::getIndexTable();
  return kvstore->backupTable(
      kDefaultSpaceId,
      backupName,
      indexTable,
      [spaces, spaceName, indexTable](const folly::StringPiece& key) -> bool {
        if (spaces.empty()) {
          return false;
        }

        auto type = *reinterpret_cast<const EntryType*>(key.data() + indexTable.size());
        if (type == EntryType::SPACE) {
          if (spaceName == nullptr) {
            return false;
          }
          auto sn = key.subpiece(indexTable.size() + sizeof(EntryType),
                                 key.size() - indexTable.size() - sizeof(EntryType))
                        .str();
          LOG(INFO) << "sn was " << sn;
          auto it = std::find_if(
              spaceName->cbegin(), spaceName->cend(), [&sn](auto& name) { return sn == name; });

          if (it == spaceName->cend()) {
            return true;
          }
          return false;
        }

        auto id = MetaKeyUtils::parseIndexKeySpaceID(key);
        auto it = spaces.find(id);
        if (it == spaces.end()) {
          return true;
        }

        return false;
      });
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> MetaServiceUtils::backupSpaces(
    kvstore::KVStore* kvstore,
    const std::unordered_set<GraphSpaceID>& spaces,
    const std::string& backupName,
    const std::vector<std::string>* spaceNames) {
  std::vector<std::string> files;
  auto tables = MetaKeyUtils::getTableMaps();
  files.reserve(tables.size());

  for (const auto& table : tables) {
    if (table.second.second == nullptr) {
      LOG(INFO) << table.first << " table skipped";
      continue;
    }
    auto result = backupTable(
        kvstore, backupName, table.second.first, files, spaceFilter(spaces, table.second.second));
    if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return result;
    }
    LOG(INFO) << table.first << " table backup succeeded";
  }

  if (spaceNames == nullptr) {
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

  // The mapping of space name and space id needs to be handled separately.
  auto ret = backupIndex(kvstore, spaces, backupName, spaceNames);
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
