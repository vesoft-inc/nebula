/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/MaintainSentences.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"

namespace nebula {

std::ostream& operator<<(std::ostream& os, nebula::cpp2::PropertyType type) {
  os << apache::thrift::util::enumNameSafe(type);
  return os;
}

std::string SchemaPropItem::toString() const {
  switch (propType_) {
    case TTL_DURATION:
      return folly::stringPrintf("ttl_duration = %ld", std::get<int64_t>(propValue_));
    case TTL_COL:
      return folly::stringPrintf("ttl_col = \"%s\"", std::get<std::string>(propValue_).c_str());
    case COMMENT:
      return folly::stringPrintf("comment = \"%s\"", std::get<std::string>(propValue_).c_str());
  }
  DLOG(FATAL) << "Schema property type illegal";
  return "";
}

std::string SchemaPropList::toString() const {
  std::string buf;
  buf.reserve(256);
  for (auto& item : items_) {
    buf += " ";
    buf += item->toString();
    buf += ",";
  }
  if (!buf.empty()) {
    buf.resize(buf.size() - 1);
  }
  return buf;
}

std::string ColumnProperty::toString() const {
  std::stringstream str;
  if (isNullable()) {
    if (nullable()) {
      str << "NULL";
    } else {
      str << "NOT NULL";
    }
  } else if (isDefaultValue()) {
    str << "DEFAULT " << DCHECK_NOTNULL(defaultValue())->toString();
  } else if (isComment()) {
    str << "COMMENT '" << *DCHECK_NOTNULL(comment()) << "'";
  }
  return str.str();
}

std::string ColumnSpecification::toString() const {
  std::string buf;
  buf.reserve(128);
  buf += "`";
  buf += *name_;
  buf += "` ";
  if (nebula::cpp2::PropertyType::FIXED_STRING == type_) {
    buf += "FIXED_STRING(";
    buf += std::to_string(typeLen_);
    buf += ")";
  } else {
    buf += apache::thrift::util::enumNameSafe(type_);
  }
  buf += " ";
  buf += properties_->toString();
  return buf;
}

std::string ColumnSpecificationList::toString() const {
  std::string buf;
  buf.reserve(128);
  const auto& colSpecs = columnSpecs();
  for (auto& col : colSpecs) {
    buf += col->toString();
    buf += ",";
  }
  if (!colSpecs.empty()) {
    buf.resize(buf.size() - 1);
  }
  return buf;
}

std::string CreateTagSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "CREATE TAG ";
  if (isIfNotExist()) {
    buf += "IF NOT EXISTS ";
  }
  buf += "`";
  buf += *name_;
  buf += "` (";
  buf += columns_->toString();
  buf += ")";
  if (schemaProps_ != nullptr) {
    buf += schemaProps_->toString();
  }
  return buf;
}

std::string CreateEdgeSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "CREATE EDGE ";
  if (isIfNotExist()) {
    buf += "IF NOT EXISTS ";
  }
  buf += "`";
  buf += *name_;
  buf += "` (";
  buf += columns_->toString();
  buf += ")";
  if (schemaProps_ != nullptr) {
    buf += schemaProps_->toString();
  }
  return buf;
}

std::string AlterSchemaOptItem::toString() const {
  std::string buf;
  buf.reserve(256);
  switch (optType_) {
    case ADD:
      buf += "ADD";
      buf += " (";
      if (columns_ != nullptr) {
        buf += columns_->toString();
      }
      buf += ")";
      break;
    case CHANGE:
      buf += "CHANGE";
      buf += " (";
      if (columns_ != nullptr) {
        buf += columns_->toString();
      }
      buf += ")";
      break;
    case DROP:
      buf += "DROP";
      buf += " (";
      if (colNames_ != nullptr) {
        buf += colNames_->toString();
      }
      buf += ")";
      break;
  }
  return buf;
}

nebula::meta::cpp2::AlterSchemaOp AlterSchemaOptItem::toType() {
  switch (optType_) {
    case ADD:
      return nebula::meta::cpp2::AlterSchemaOp::ADD;
    case CHANGE:
      return nebula::meta::cpp2::AlterSchemaOp::CHANGE;
    case DROP:
      return nebula::meta::cpp2::AlterSchemaOp::DROP;
    default:
      return nebula::meta::cpp2::AlterSchemaOp::UNKNOWN;
  }
}

std::string AlterSchemaOptList::toString() const {
  std::string buf;
  buf.reserve(256);
  for (auto& item : alterSchemaItems_) {
    buf += " ";
    buf += item->toString();
    buf += ",";
  }
  if (!buf.empty()) {
    buf.pop_back();
  }
  return buf;
}

std::string AlterTagSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "ALTER TAG ";
  buf += *name_;
  if (opts_ != nullptr) {
    buf += " ";
    buf += opts_->toString();
  }
  if (schemaProps_ != nullptr) {
    buf += schemaProps_->toString();
  }
  return buf;
}

std::string AlterEdgeSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "ALTER EDGE ";
  buf += *name_;
  if (opts_ != nullptr) {
    buf += " ";
    buf += opts_->toString();
  }
  if (schemaProps_ != nullptr) {
    buf += schemaProps_->toString();
  }
  return buf;
}

std::string DescribeTagSentence::toString() const {
  return folly::stringPrintf("DESCRIBE TAG %s", name_.get()->c_str());
}

std::string DescribeEdgeSentence::toString() const {
  return folly::stringPrintf("DESCRIBE EDGE %s", name_.get()->c_str());
}

std::string DropTagSentence::toString() const {
  return folly::stringPrintf("DROP TAG %s", name_.get()->c_str());
}

std::string DropEdgeSentence::toString() const {
  return folly::stringPrintf("DROP EDGE %s", name_.get()->c_str());
}

std::string IndexParamItem::toString() const {
  switch (paramType_) {
    case S2_MAX_LEVEL:
      return folly::stringPrintf("s2_max_level = %ld", paramValue_.getInt());
    case S2_MAX_CELLS:
      return folly::stringPrintf("s2_max_cells = \"%ld\"", paramValue_.getInt());
  }
  DLOG(FATAL) << "Index param type illegal";
  return "";
}

std::string IndexParamList::toString() const {
  std::string buf;
  buf.reserve(256);
  for (auto& item : items_) {
    buf += " ";
    buf += item->toString();
    buf += ",";
  }
  if (!buf.empty()) {
    buf.resize(buf.size() - 1);
  }
  return buf;
}

std::string CreateTagIndexSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "CREATE TAG INDEX ";
  if (isIfNotExist()) {
    buf += "IF NOT EXISTS ";
  }
  buf += *indexName_;
  buf += " ON ";
  buf += *tagName_;
  buf += "(";
  std::vector<std::string> fieldDefs;
  for (const auto& field : this->fields()) {
    std::string f = field.get_name();
    if (field.type_length_ref().has_value()) {
      f += "(" + std::to_string(*field.type_length_ref()) + ")";
    }
    fieldDefs.emplace_back(std::move(f));
  }
  std::string fields;
  folly::join(", ", fieldDefs, fields);
  buf += fields;
  buf += ")";
  std::string params;
  if (indexParams_ != nullptr) {
    params = indexParams_->toString();
  }
  if (!params.empty()) {
    buf += "WITH (";
    buf += params;
    buf += ")";
  }
  if (comment_ != nullptr) {
    buf += "COMMENT ";
    buf += *comment_;
  }
  return buf;
}

std::string CreateEdgeIndexSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "CREATE EDGE INDEX ";
  if (isIfNotExist()) {
    buf += "IF NOT EXISTS ";
  }
  buf += *indexName_;
  buf += " ON ";
  buf += *edgeName_;
  buf += "(";
  std::vector<std::string> fieldDefs;
  for (const auto& field : this->fields()) {
    std::string f = field.get_name();
    if (field.type_length_ref().has_value()) {
      f += "(" + std::to_string(*field.type_length_ref()) + ")";
    }
    fieldDefs.emplace_back(std::move(f));
  }
  std::string fields;
  folly::join(", ", fieldDefs, fields);
  buf += fields;
  buf += ")";
  std::string params;
  if (indexParams_ != nullptr) {
    params = indexParams_->toString();
  }
  if (!params.empty()) {
    buf += "WITH (";
    buf += params;
    buf += ")";
  }
  if (comment_ != nullptr) {
    buf += "COMMENT ";
    buf += *comment_;
  }
  return buf;
}

std::string DescribeTagIndexSentence::toString() const {
  return folly::stringPrintf("DESCRIBE TAG INDEX %s", indexName_.get()->c_str());
}

std::string DescribeEdgeIndexSentence::toString() const {
  return folly::stringPrintf("DESCRIBE EDGE INDEX %s", indexName_.get()->c_str());
}

std::string DropTagIndexSentence::toString() const {
  return folly::stringPrintf("DROP TAG INDEX %s", indexName_.get()->c_str());
}

std::string DropEdgeIndexSentence::toString() const {
  return folly::stringPrintf("DROP EDGE INDEX %s", indexName_.get()->c_str());
}

std::string ShowTagsSentence::toString() const {
  return folly::stringPrintf("SHOW TAGS");
}

std::string ShowEdgesSentence::toString() const {
  return folly::stringPrintf("SHOW EDGES");
}

std::string ShowCreateTagSentence::toString() const {
  return folly::stringPrintf("SHOW CREATE TAG %s", name_.get()->c_str());
}

std::string ShowCreateEdgeSentence::toString() const {
  return folly::stringPrintf("SHOW CREATE EDGE %s", name_.get()->c_str());
}

std::string ShowTagIndexesSentence::toString() const {
  std::string buf;
  buf.reserve(64);
  buf += "SHOW TAG INDEXES";
  if (!name()->empty()) {
    buf += " BY ";
    buf += *name_;
  }
  return buf;
}

std::string ShowEdgeIndexesSentence::toString() const {
  std::string buf;
  buf.reserve(64);
  buf += "SHOW EDGE INDEXES";
  if (!name()->empty()) {
    buf += " BY ";
    buf += *name_;
  }
  return buf;
}

std::string ShowTagIndexStatusSentence::toString() const {
  return folly::stringPrintf("SHOW TAG INDEX STATUS");
}

std::string ShowEdgeIndexStatusSentence::toString() const {
  return folly::stringPrintf("SHOW EDGE INDEX STATUS");
}

std::string ShowCreateTagIndexSentence::toString() const {
  return folly::stringPrintf("SHOW CREATE TAG INDEX %s", indexName_.get()->c_str());
}

std::string ShowCreateEdgeIndexSentence::toString() const {
  return folly::stringPrintf("SHOW CREATE EDGE INDEX %s", indexName_.get()->c_str());
}

std::string AddHostsSentence::toString() const {
  std::string buf;
  buf.reserve(64);
  buf += "ADD HOSTS ";
  buf += hosts_->toString();
  return buf;
}

std::string DropHostsSentence::toString() const {
  std::string buf;
  buf += "DROP HOSTS ";
  buf += hosts_->toString();
  buf.reserve(64);
  return buf;
}

std::string MergeZoneSentence::toString() const {
  std::string buf;
  buf.reserve(128);
  buf += "MERGE ZONE ";
  buf += zoneNames_->toString();
  buf += " INTO `";
  buf += *zoneName_;
  buf += "`";
  return buf;
}

std::string DropZoneSentence::toString() const {
  return folly::stringPrintf("DROP ZONE `%s`", zoneName_.get()->c_str());
}

std::string DivideZoneSentence::toString() const {
  std::string buf;
  buf.reserve(128);
  buf += "DIVIDE ZONE `";
  buf += *zoneName_;
  buf += "` INTO ";
  buf += zoneItems_->toString();
  return buf;
}

std::string RenameZoneSentence::toString() const {
  std::string buf;
  buf.reserve(128);
  buf += "RENAME ZONE `";
  buf += *originalZoneName_;
  buf += "` TO `";
  buf += *zoneName_;
  buf += "`";
  return buf;
}

std::string DescribeZoneSentence::toString() const {
  return folly::stringPrintf("DESCRIBE ZONE `%s`", zoneName_.get()->c_str());
}

std::string ShowZonesSentence::toString() const {
  return folly::stringPrintf("SHOW ZONES");
}

std::string AddHostsIntoZoneSentence::toString() const {
  std::string buf;
  buf.reserve(64);
  buf += "ADD HOSTS ";
  buf += address_->toString();
  if (isNew_) {
    buf += " INTO NEW ZONE `";
  } else {
    buf += " INTO ZONE `";
  }
  buf += *zoneName_;
  buf += "`";
  return buf;
}

std::string CreateFTIndexSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "CREATE FULLTEXT ";
  if (isEdge_) {
    buf += "EDGE";
  } else {
    buf += "TAG";
  }
  buf += " INDEX ";
  buf += *indexName_;
  buf += " ON ";
  buf += *schemaName_;
  buf += "(";
  std::vector<std::string> fieldDefs;
  fieldDefs.emplace_back(field());
  std::string fields;
  folly::join(", ", fieldDefs, fields);
  buf += fields;
  buf += ")";
  return buf;
}

std::string DropFTIndexSentence::toString() const {
  return folly::stringPrintf("DROP FULLTEXT INDEX %s", indexName_.get()->c_str());
}

std::string ShowFTIndexesSentence::toString() const {
  return "SHOW FULLTEXT INDEXES";
}

}  // namespace nebula
