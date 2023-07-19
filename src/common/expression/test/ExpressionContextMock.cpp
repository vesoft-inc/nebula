/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/test/ExpressionContextMock.h"

#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

std::unordered_map<std::string, std::size_t> ExpressionContextMock::indices_ = {
    {"empty", 0},     {"null", 1},          {"bool_true", 2},    {"bool_false", 3},
    {"int", 4},       {"float", 5},         {"string16", 6},     {"string32", 7},
    {"string64", 8},  {"string128", 9},     {"string256", 10},   {"string4096", 11},
    {"list", 12},     {"list_of_list", 13}, {"var_int", 14},     {"versioned_var", 15},
    {"cnt", 16},      {"_src", 17},         {"_dst", 18},        {"_type", 19},
    {"_rank", 20},    {"srcProperty", 21},  {"dstProperty", 22},

    {"path_src", 23}, {"path_edge1", 24},   {"path_v1", 25},     {"path_edge2", 26},
    {"path_v2", 27},  {"path_edge3", 28},
};

std::vector<Value> ExpressionContextMock::vals_ = {
    Value(),
    Value(NullType::NaN),
    Value(true),
    Value(false),
    Value(1),
    Value(1.1),
    Value(std::string(16, 'a')),
    Value(std::string(32, 'a')),
    Value(std::string(64, 'a')),
    Value(std::string(128, 'a')),
    Value(std::string(256, 'a')),
    Value(std::string(4096, 'a')),
    Value(List(std::vector<Value>(16, Value("aaaa")))),
    Value(List(std::vector<Value>(16, Value(List(std::vector<Value>(16, Value("aaaa"))))))),
    Value(1),
    Value(List(std::vector<Value>{1, 2, 3, 4, 5, 6, 7, 8})),
    Value(1),
    Value(1),
    Value(1),
    Value(1),
    Value(1),
    Value(13),
    Value(3),

    Value(Vertex("1", {})),
    Value(Edge("1", "2", 1, "edge", 0, {})),
    Value(Vertex("2", {})),
    Value(Edge("2", "3", 1, "edge", 0, {})),
    Value(Vertex("3", {})),
    Value(Edge("3", "4", 1, "edge", 0, {})),
};

const Value& ExpressionContextMock::getColumn(int32_t index) const {
  auto size = vals_.size();
  if (static_cast<size_t>(std::abs(index)) >= size) {
    return Value::kNullBadType;
  }
  return vals_[(size + index) % size];
}

}  // namespace nebula
