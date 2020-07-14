/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DescribeSchemaExecutor.h"

namespace nebula {
namespace graph {

/*static*/ std::string DescribeSchemaExecutor::value2String(const ::nebula::cpp2::Value& value) {
    switch (value.getType()) {
        case ::nebula::cpp2::Value::Type::bool_value: {
            return std::to_string(value.get_bool_value());
        }
        case ::nebula::cpp2::Value::Type::int_value: {
            return std::to_string(value.get_int_value());
        }
        case ::nebula::cpp2::Value::Type::double_value: {
            return std::to_string(value.get_double_value());
        }
        case ::nebula::cpp2::Value::Type::string_value: {
            return "\"" + value.get_string_value() + "\"";
        }
        case ::nebula::cpp2::Value::Type::timestamp: {
            return std::to_string(value.get_timestamp());
        }
        case ::nebula::cpp2::Value::Type::__EMPTY__: {
            // TODO Default NULL if NULL introduced
            return std::string();
        }
    }
    return std::string();
}

/*static*/ std::vector<cpp2::RowValue> DescribeSchemaExecutor::genRows(
    const std::vector<::nebula::cpp2::ColumnDef>& cols) {
    std::vector<cpp2::RowValue> rows;
    for (auto& item : cols) {
        std::vector<cpp2::ColumnValue> row;
        row.resize(6);
        // Field
        row[0].set_str(item.name);
        // Type
        row[1].set_str(valueTypeToString(item.type));
        // Null
        if (item.get_could_null() != nullptr) {
            row[2].set_bool_val(*item.get_could_null());
        } else {
            // For testing checking with one value
            row[2].set_bool_val(false);
        }
        // Key
        if (item.get_key_type() != nullptr) {
            auto keyType = *item.get_key_type();
            auto keyTypeName = keyTypeToString(keyType);
            row[3].set_str(keyTypeName);
        } else {
            // For testing checking with one value
            row[3].set_str(std::string());
        }
        // Default
        if (item.get_default_value() != nullptr) {
            // covert to characters for the column require same type for testing checking
            // TODO make the column value typed if column with different type supported
            row[4].set_str(value2String(*item.get_default_value()));
        } else {
            // For testing checking with one value
            // TODO Default NULL if NULL introduced
            row[4].set_str(std::string());
        }
        // Extra TODO(shylock) reserved now
        row[5].set_str(std::string());
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
    }
    return rows;
}

}  // namespace graph
}  // namespace nebula
