/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/test/ExpressionContextMock.h"

#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

std::unordered_map<std::string, Value> ExpressionContextMock::vals_ = {
    {"empty", Value()},
    {"null", Value(NullType::NaN)},
    {"bool_true", Value(true)},
    {"bool_false", Value(false)},
    {"int", Value(1)},
    {"float", Value(1.1)},
    {"string16", Value(std::string(16, 'a'))},
    {"string32", Value(std::string(32, 'a'))},
    {"string64", Value(std::string(64, 'a'))},
    {"string128", Value(std::string(128, 'a'))},
    {"string256", Value(std::string(256, 'a'))},
    {"string4096", Value(std::string(4096, 'a'))},
    {"list", Value(List(std::vector<Value>(16, Value("aaaa"))))},
    {"list_of_list",
     Value(List(std::vector<Value>(16, Value(List(std::vector<Value>(16, Value("aaaa")))))))},
    {"var_int", Value(1)},
    {"versioned_var", Value(List(std::vector<Value>{1, 2, 3, 4, 5, 6, 7, 8}))},
    {"cnt", Value(1)},
    {"_src", Value(1)},
    {"_dst", Value(1)},
    {"_type", Value(1)},
    {"_rank", Value(1)},
    {"srcProperty", Value(13)},
    {"dstProperty", Value(3)},

    {"path_src", Value("1")},
    {"path_edge1", Value(Edge("1", "2", 1, "edge", 0, {}))},
    {"path_v1", Value("2")},
    {"path_edge2", Value(Edge("2", "3", 1, "edge", 0, {}))},
    {"path_v2", Value(Vertex("3", {}))},
    {"path_edge3", Value(Edge("3", "4", 1, "edge", 0, {}))},
};

Value ExpressionContextMock::getColumn(int32_t index) const {
    auto row = vals_["versioned_var"].getList().values;
    auto size = row.size();
    if (static_cast<size_t>(std::abs(index)) >= size) {
        return Value::kNullBadType;
    }
    return row[(size + index) % size];
}

}   // namespace nebula
