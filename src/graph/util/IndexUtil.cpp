/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/util/IndexUtil.h"

#include <string>

namespace nebula {
namespace graph {

Status IndexUtil::validateColumns(const std::vector<std::string> &fields) {
  std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
  if (fieldSet.size() != fields.size()) {
    return Status::Error("Found duplicate column field");
  }

  if (fields.empty()) {
    return Status::Error("Column is empty");
  }

  return Status::OK();
}

// static
Status IndexUtil::validateIndexParams(const std::vector<IndexParamItem *> &params,
                                      meta::cpp2::IndexParams &indexParams) {
  for (auto *param : params) {
    switch (param->getParamType()) {
      case IndexParamItem::COMMENT: {
        auto ret = param->getComment();
        NG_RETURN_IF_ERROR(ret);
        indexParams.set_comment(std::move(ret).value());
        break;
      }
      case IndexParamItem::S2_MAX_LEVEL: {
        auto ret2 = param->getS2MaxLevel();
        NG_RETURN_IF_ERROR(ret2);
        indexParams.set_s2_max_level(std::move(ret2).value());
        break;
      }
      case IndexParamItem::S2_MAX_CELLS: {
        auto ret3 = param->getS2MaxCells();
        NG_RETURN_IF_ERROR(ret3);
        indexParams.set_s2_max_cells(std::move(ret3).value());
        break;
      }
    }
  }

  return Status::OK();
}

StatusOr<DataSet> IndexUtil::toDescIndex(const meta::cpp2::IndexItem &indexItem) {
  DataSet dataSet({"Field", "Type"});
  for (auto &col : indexItem.get_fields()) {
    Row row;
    row.values.emplace_back(Value(col.get_name()));
    row.values.emplace_back(SchemaUtil::typeToString(col));
    dataSet.emplace_back(std::move(row));
  }
  return dataSet;
}

StatusOr<DataSet> IndexUtil::toShowCreateIndex(bool isTagIndex,
                                               const std::string &indexName,
                                               const meta::cpp2::IndexItem &indexItem) {
  DataSet dataSet;
  std::string createStr;
  createStr.reserve(1024);
  std::string schemaName = indexItem.get_schema_name();
  if (isTagIndex) {
    dataSet.colNames = {"Tag Index Name", "Create Tag Index"};
    createStr = "CREATE TAG INDEX `" + indexName + "` ON `" + schemaName + "` (\n";
  } else {
    dataSet.colNames = {"Edge Index Name", "Create Edge Index"};
    createStr = "CREATE EDGE INDEX `" + indexName + "` ON `" + schemaName + "` (\n";
  }
  Row row;
  row.emplace_back(indexName);
  for (auto &col : indexItem.get_fields()) {
    createStr += " `" + col.get_name();
    createStr += "`";
    const auto &type = col.get_type();
    if (type.type_length_ref().has_value()) {
      createStr += "(" + std::to_string(*type.type_length_ref()) + ")";
    }
    createStr += ",\n";
  }
  if (!(*indexItem.fields_ref()).empty()) {
    createStr.resize(createStr.size() - 2);
    createStr += "\n";
  }
  createStr += ")";
  std::vector<std::string> indexParams;
  if (indexItem.comment_ref().has_value()) {
    indexParams.emplace_back("comment = \"" + *indexItem.comment_ref() + "\"");
  }
  if (indexItem.s2_max_level_ref().has_value()) {
    indexParams.emplace_back("s2_max_level = " + std::to_string(*indexItem.s2_max_level_ref()));
  }
  if (indexItem.s2_max_cells_ref().has_value()) {
    indexParams.emplace_back("s2_max_cells = " + std::to_string(*indexItem.s2_max_cells_ref()));
  }
  if (!indexParams.empty()) {
    createStr += " ";
    createStr += folly::join(", ", indexParams);
  }

  row.emplace_back(std::move(createStr));
  dataSet.rows.emplace_back(std::move(row));
  return dataSet;
}

Expression::Kind IndexUtil::reverseRelationalExprKind(Expression::Kind kind) {
  switch (kind) {
    case Expression::Kind::kRelGE: {
      return Expression::Kind::kRelLE;
    }
    case Expression::Kind::kRelGT: {
      return Expression::Kind::kRelLT;
    }
    case Expression::Kind::kRelLE: {
      return Expression::Kind::kRelGE;
    }
    case Expression::Kind::kRelLT: {
      return Expression::Kind::kRelGT;
    }
    default: {
      return kind;
    }
  }
}

}  // namespace graph
}  // namespace nebula
