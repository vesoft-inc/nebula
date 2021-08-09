/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/IndexUtil.h"

namespace nebula {
namespace graph {

Status IndexUtil::validateColumns(const std::vector<std::string>& fields) {
    std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
    if (fieldSet.size() != fields.size()) {
        return Status::Error("Found duplicate column field");
    }

    if (fields.empty()) {
        return Status::Error("Column is empty");
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
        createStr = "CREATE TAG INDEX `" + indexName +  "` ON `" + schemaName + "` (\n";
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
        createStr.resize(createStr.size() -2);
        createStr += "\n";
    }
    createStr += ")";
    if (indexItem.comment_ref().has_value()) {
        createStr += " comment = \"";
        createStr += *indexItem.get_comment();
        createStr += "\"";
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
