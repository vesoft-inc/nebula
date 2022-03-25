/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/test/TestBase.h"

namespace nebula {

ExpressionContextMock gExpCtxt;
ObjectPool pool;

std::unordered_map<std::string, std::vector<Value>> args_ = {
    {"null", {}},
    {"int", {4}},
    {"float", {1.1}},
    {"neg_int", {-1}},
    {"neg_float", {-1.1}},
    {"rand", {1, 10}},
    {"one", {-1.2}},
    {"two", {2, 4}},
    {"pow", {2, 3}},
    {"string", {"AbcDeFG"}},
    {"trim", {" abc  "}},
    {"substr", {"abcdefghi", 2, 4}},
    {"side", {"abcdefghijklmnopq", 5}},
    {"neg_side", {"abcdefghijklmnopq", -2}},
    {"pad", {"abcdefghijkl", 16, "123"}},
    {"udf_is_in", {4, 1, 2, 8, 4, 3, 1, 0}},
};

}  // namespace nebula
