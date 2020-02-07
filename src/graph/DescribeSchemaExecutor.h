/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DescribeSchemaExecutor : public Executor {
protected:
    DescribeSchemaExecutor(ExecutionContext* ectx, const std::string& name)
        : Executor(ectx, name) {}

    static const char* keyTypeToString(::nebula::cpp2::KeyType type) {
        switch (type) {
            case ::nebula::cpp2::KeyType::PRI:
                return "PRI";
            case ::nebula::cpp2::KeyType::UNI:
                return "UNI";
            case ::nebula::cpp2::KeyType::MUL:
                return "MUL";
            case ::nebula::cpp2::KeyType::EMPTY:
                return "";
        }
        return "";
    }

    // convert nebula value to string
    static std::string value2String(const ::nebula::cpp2::Value& value);

    static std::vector<cpp2::RowValue> genRows(const std::vector<::nebula::cpp2::ColumnDef>& cols);

    // Describe schema header
    const std::vector<std::string> header_{"Field", "Type", "Null", "Key", "Default", "Extra"};
};

}  // namespace graph
}  // namespace nebula
