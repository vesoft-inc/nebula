/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/time/TimezoneInfo.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Vertex.h"
#include "common/function/FunctionManager.h"
#include "common/time/TimeUtils.h"

namespace nebula {

class FunctionManagerTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    ::testing::AssertionResult testFunction(const char *expr,
                                            const std::vector<Value> &args,
                                            Value expect) {
        auto argsRef = genArgsRef(args);
        auto result = FunctionManager::get(expr, args.size());
        if (!result.ok()) {
            return ::testing::AssertionFailure()
                   << "Can't get fuction " << expr << " with " << args.size() << " parameters.";
        }
        auto res = result.value()(argsRef);
        if (res.type() != expect.type()) {
            return ::testing::AssertionFailure() << "function return type check failed: " << expr;
        }
        if (res != expect) {
            return ::testing::AssertionFailure() << "function return value check failed: " << expr;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult testFunction(const char *expr,
                                            const std::vector<Value> &args,
                                            Value::Type expectType) {
        auto argsRef = genArgsRef(args);
        auto result = FunctionManager::get(expr, args.size());
        if (!result.ok()) {
            return ::testing::AssertionFailure()
                   << "Can't get fuction " << expr << " with " << args.size() << " parameters.";
        }
        auto res = result.value()(argsRef);
        if (res.type() != expectType) {
            return ::testing::AssertionFailure() << "function return type check failed: " << expr;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult testFunction(const char *expr, const std::vector<Value> &args) {
        std::vector<FunctionManager::ArgType> argsRef;
        argsRef.insert(argsRef.end(), args.begin(), args.end());
        auto result = FunctionManager::get(expr, args.size());
        if (!result.ok()) {
            return ::testing::AssertionFailure()
                   << "Can't get fuction " << expr << " with " << args.size() << " parameters.";
        }
        return ::testing::AssertionSuccess();
    }

    std::vector<FunctionManager::ArgType> genArgsRef(const std::vector<Value> &args) {
        std::vector<FunctionManager::ArgType> argsRef;
        argsRef.insert(argsRef.end(), args.begin(), args.end());
        return argsRef;
    }

    Path createPath(const Value &src, const std::vector<Value> &steps) {
        Path path;
        path.src = Vertex(src, {});
        for (auto &i : steps) {
            path.steps.emplace_back(Step(Vertex(i, {}), 1, "edge1", 0, {}));
        }
        return path;
    }

    static std::unordered_map<std::string, std::vector<Value>> args_;
};

std::unordered_map<std::string, std::vector<Value>> FunctionManagerTest::args_ = {
    {"empty", {}},
    {"nullvalue", {NullType::__NULL__}},
    {"int", {4}},
    {"zero", {0}},
    {"float", {1.1}},
    {"neg_int", {-1}},
    {"neg_float", {-1.1}},
    {"rand", {1, 10}},
    {"one", {-1.2}},
    {"two", {2, 4}},
    {"pow", {2, 3}},
    {"radians", {180}},
    {"range1", {1, 5}},
    {"range2", {1, 5, 2}},
    {"range3", {5, 1, -2}},
    {"range4", {1, 5, -2}},
    {"range5", {5, 1, 2}},
    {"range6", {1, 5, 0}},
    {"string", {"AbcDeFG"}},
    {"trim", {" abc  "}},
    {"substr", {"abcdefghi", 2, 4}},
    {"substring", {"abcdef", 2}},
    {"substring_outRange", {"abcdef", 10}},
    {"replace", {"abcdefghi", "cde", "ZZZ"}},
    {"reverse", {"qwerty"}},
    {"split", {"//nebula//graph//database//", "//"}},
    {"toString_bool", {true}},
    {"toString_float", {1.233}},
    {"toString_float2", {1.0}},
    {"side", {"abcdefghijklmnopq", 5}},
    {"neg_side", {"abcdefghijklmnopq", -2}},
    {"pad", {"abcdefghijkl", 16, "123"}},
    {"udf_is_in", {4, 1, 2, 8, 4, 3, 1, 0}},
    {"date", {Date(1984, 10, 11)}},
    {"datetime", {DateTime(1984, 10, 11, 12, 31, 14, 341)}},
    {"edge", {Edge("1", "2", -1, "e1", 0, {{"e1", 1}, {"e2", 2}})}},
};

#define TEST_FUNCTION(expr, ...)                                                                   \
    do {                                                                                           \
        EXPECT_TRUE(testFunction(#expr, __VA_ARGS__));                                             \
    } while (0);

TEST_F(FunctionManagerTest, testNull) {
    // scalar functions
    TEST_FUNCTION(coalesce, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(endNode, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(startNode, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(head, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(id, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(last, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(length, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(properties, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(size, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toBoolean, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toString, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toFloat, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toInteger, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(type, args_["nullvalue"], Value::kNullValue);

    // list functions
    TEST_FUNCTION(keys, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(labels, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(nodes, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(relationships, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(reverse, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(tail, args_["nullvalue"], Value::kNullValue);

    // mathematical function
    TEST_FUNCTION(abs, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(ceil, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(floor, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(round, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(sign, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(exp, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(log, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(log2, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(log10, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(sqrt, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(acos, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(asin, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(atan, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(cos, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(sin, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(tan, args_["nullvalue"], Value::kNullValue);

    // string functions
    TEST_FUNCTION(left, std::vector<Value>({Value::kNullValue, 3}), Value::kNullValue);
    TEST_FUNCTION(
        left, std::vector<Value>({Value::kNullValue, Value::kNullValue}), Value::kNullValue);
    TEST_FUNCTION(left, std::vector<Value>({"abc", Value::kNullValue}), Value::kNullBadType);
    TEST_FUNCTION(left, std::vector<Value>({"abc", -2}), Value::kNullBadType);
    TEST_FUNCTION(right, std::vector<Value>({Value::kNullValue, 3}), Value::kNullValue);
    TEST_FUNCTION(
        right, std::vector<Value>({Value::kNullValue, Value::kNullValue}), Value::kNullValue);
    TEST_FUNCTION(right, std::vector<Value>({"abc", Value::kNullValue}), Value::kNullBadType);
    TEST_FUNCTION(right, std::vector<Value>({"abc", -2}), Value::kNullBadType);
    TEST_FUNCTION(ltrim, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(rtrim, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(trim, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(reverse, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toLower, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(toUpper, args_["nullvalue"], Value::kNullValue);
    TEST_FUNCTION(split, std::vector<Value>({Value::kNullValue, ","}), Value::kNullValue);
    TEST_FUNCTION(split, std::vector<Value>({"123,22", Value::kNullValue}), Value::kNullValue);
    TEST_FUNCTION(substr, std::vector<Value>({Value::kNullValue, 1, 2}), Value::kNullValue);
    TEST_FUNCTION(substr, std::vector<Value>({"hello", Value::kNullValue, 2}), Value::kNullBadType);
    TEST_FUNCTION(substr, std::vector<Value>({"hello", 2, Value::kNullValue}), Value::kNullBadType);
    TEST_FUNCTION(substr, std::vector<Value>({"hello", -1, 10}), Value::kNullBadData);
    TEST_FUNCTION(substr, std::vector<Value>({"hello", 1, -2}), Value::kNullBadData);
}

TEST_F(FunctionManagerTest, functionCall) {
    {
        TEST_FUNCTION(abs, args_["neg_int"], 1);
        TEST_FUNCTION(abs, args_["neg_float"], 1.1);
        TEST_FUNCTION(abs, args_["int"], 4);
        TEST_FUNCTION(abs, args_["float"], 1.1);
    }
    {
        TEST_FUNCTION(bit_and, args_["two"], 0);
        TEST_FUNCTION(bit_or, args_["two"], 6);
        TEST_FUNCTION(bit_xor, args_["two"], 6);

        TEST_FUNCTION(bit_and, std::vector<Value>({"abc", -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_and, std::vector<Value>({2.1, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_and, std::vector<Value>({true, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_and, std::vector<Value>({Value::kNullValue, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_and, std::vector<Value>({Value::kEmpty, -2}), Value::kEmpty);

        TEST_FUNCTION(bit_or, std::vector<Value>({"abc", -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_or, std::vector<Value>({2.1, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_or, std::vector<Value>({true, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_or, std::vector<Value>({Value::kNullValue, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_or, std::vector<Value>({Value::kEmpty, -2}), Value::kEmpty);

        TEST_FUNCTION(bit_xor, std::vector<Value>({"abc", -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_xor, std::vector<Value>({2.1, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_xor, std::vector<Value>({true, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_xor, std::vector<Value>({Value::kNullValue, -2}), Value::kNullBadType);
        TEST_FUNCTION(bit_xor, std::vector<Value>({Value::kEmpty, -2}), Value::kEmpty);
    }
    {
        TEST_FUNCTION(floor, args_["neg_int"], -1.0);
        TEST_FUNCTION(floor, args_["float"], 1.0);
        TEST_FUNCTION(floor, args_["neg_float"], -2.0);
        TEST_FUNCTION(floor, args_["int"], 4.0);
    }
    {
        TEST_FUNCTION(sqrt, args_["int"], 2.0);
        TEST_FUNCTION(sqrt, args_["float"], std::sqrt(1.1));
    }
    {
        TEST_FUNCTION(cbrt, args_["int"], std::cbrt(4));
        TEST_FUNCTION(cbrt, args_["float"], std::cbrt(1.1));
    }
    { TEST_FUNCTION(hypot, args_["two"], std::hypot(2, 4)); }
    {
        TEST_FUNCTION(sign, args_["int"], 1);
        TEST_FUNCTION(sign, args_["neg_int"], -1);
        TEST_FUNCTION(sign, args_["float"], 1);
        TEST_FUNCTION(sign, args_["neg_float"], -1);
        TEST_FUNCTION(sign, args_["zero"], 0);
    }
    {
        TEST_FUNCTION(pow, args_["pow"], 8);
        TEST_FUNCTION(exp, args_["int"], std::exp(4));
        TEST_FUNCTION(exp2, args_["int"], 16.0);

        TEST_FUNCTION(log, args_["int"], std::log(4));
        TEST_FUNCTION(log2, args_["int"], 2.0);
    }
    {
        TEST_FUNCTION(range, args_["range1"], Value(List({1, 2, 3, 4, 5})));
        TEST_FUNCTION(range, args_["range2"], Value(List({1, 3, 5})));
        TEST_FUNCTION(range, args_["range3"], Value(List({5, 3, 1})));
        TEST_FUNCTION(range, args_["range4"], Value(List(std::vector<Value>{})));
        TEST_FUNCTION(range, args_["range5"], Value(List(std::vector<Value>{})));
        TEST_FUNCTION(range, args_["range6"], Value::kNullBadData);
    }
    {
        TEST_FUNCTION(lower, args_["string"], "abcdefg");
        TEST_FUNCTION(toLower, args_["string"], "abcdefg");
        TEST_FUNCTION(upper, args_["string"], "ABCDEFG");
        TEST_FUNCTION(toUpper, args_["string"], "ABCDEFG");
        TEST_FUNCTION(length, args_["string"], 7);

        TEST_FUNCTION(trim, args_["trim"], "abc");
        TEST_FUNCTION(ltrim, args_["trim"], "abc  ");
        TEST_FUNCTION(rtrim, args_["trim"], " abc");
    }
    {
        TEST_FUNCTION(substr, args_["substr"], "cdef");
        TEST_FUNCTION(substring, args_["substring"], "cdef");
        TEST_FUNCTION(substring, args_["substring_outRange"], "");
        TEST_FUNCTION(left, args_["side"], "abcde");
        TEST_FUNCTION(right, args_["side"], "mnopq");
        TEST_FUNCTION(left, args_["neg_side"], Value::kNullBadType);
        TEST_FUNCTION(right, args_["neg_side"], Value::kNullBadType);

        TEST_FUNCTION(lpad, args_["pad"], "1231abcdefghijkl");
        TEST_FUNCTION(rpad, args_["pad"], "abcdefghijkl1231");
        TEST_FUNCTION(udf_is_in, args_["udf_is_in"], true);

        TEST_FUNCTION(replace, args_["replace"], "abZZZfghi");
        TEST_FUNCTION(reverse, args_["reverse"], "ytrewq");
        TEST_FUNCTION(split, args_["split"], List({"", "nebula", "graph", "database", ""}));
        TEST_FUNCTION(toString, args_["int"], "4");
        TEST_FUNCTION(toString, args_["float"], "1.1");
        TEST_FUNCTION(toString, args_["toString_float"], "1.233");
        TEST_FUNCTION(toString, args_["toString_float2"], "1.0");
        TEST_FUNCTION(toString, args_["toString_bool"], "true");
        TEST_FUNCTION(toString, args_["string"], "AbcDeFG");
        TEST_FUNCTION(toString, args_["date"], "1984-10-11");
        TEST_FUNCTION(toString, args_["datetime"], "1984-10-11T12:31:14.341");
        TEST_FUNCTION(toString, args_["nullvalue"], Value::kNullValue);
    }
    {
        TEST_FUNCTION(toBoolean, args_["int"], Value::kNullBadType);
        TEST_FUNCTION(toBoolean, args_["float"], Value::kNullBadType);
        TEST_FUNCTION(toBoolean, {true}, true);
        TEST_FUNCTION(toBoolean, {false}, false);
        TEST_FUNCTION(toBoolean, {"fAlse"}, false);
        TEST_FUNCTION(toBoolean, {"false "}, Value::kNullValue);
        TEST_FUNCTION(toBoolean, {Value::kNullValue}, Value::kNullValue);
    }
    {
        TEST_FUNCTION(toFloat, args_["int"], 4.0);
        TEST_FUNCTION(toFloat, args_["float"], 1.1);
        TEST_FUNCTION(toFloat, {true}, Value::kNullBadType);
        TEST_FUNCTION(toFloat, {false}, Value::kNullBadType);
        TEST_FUNCTION(toFloat, {"3.14"}, 3.14);
        TEST_FUNCTION(toFloat, {"false "}, Value::kNullValue);
        TEST_FUNCTION(toFloat, {Value::kNullValue}, Value::kNullValue);
    }
    {
        TEST_FUNCTION(toInteger, args_["int"], 4);
        TEST_FUNCTION(toInteger, args_["float"], 1);
        TEST_FUNCTION(toInteger, {true}, Value::kNullBadType);
        TEST_FUNCTION(toInteger, {false}, Value::kNullBadType);
        TEST_FUNCTION(toInteger, {"1"}, 1);
        TEST_FUNCTION(toInteger, {"false "}, Value::kNullValue);
        TEST_FUNCTION(toInteger, {Value::kNullValue}, Value::kNullValue);
    }
    { TEST_FUNCTION(rand32, args_["rand"]); }
    { TEST_FUNCTION(rand32, args_["empty"]); }
    { TEST_FUNCTION(now, args_["empty"]); }
    { TEST_FUNCTION(hash, args_["string"]); }
    {
        auto result = FunctionManager::get("hash", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({"Hello"}));
        EXPECT_EQ(res, 2275118702903107253);
    }
    {
        auto result = FunctionManager::get("hash", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({3.14159265}));
        EXPECT_EQ(res, -8359970742410469755);
    }
    {
        auto result = FunctionManager::get("hash", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({1234567890}));
        EXPECT_EQ(res, 1234567890);
    }
    {
        auto result = FunctionManager::get("hash", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({true}));
        EXPECT_EQ(res, 1);
    }
    {
        auto result = FunctionManager::get("hash", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({false}));
        EXPECT_EQ(res, 0);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({List({123})}));
        EXPECT_EQ(res, 1);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({Set({123})}));
        EXPECT_EQ(res, 1);
    }
    {
        DataSet ds;
        ds.rows.emplace_back(Row({123}));
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({std::move(ds)}));
        EXPECT_EQ(res, 1);
    }
    {
        Map map;
        map.kvs.emplace("123", 456);
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({std::move(map)}));
        EXPECT_EQ(res, 1);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({"123"}));
        EXPECT_EQ(res, 3);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({Value::kNullValue}));
        EXPECT_EQ(res, Value::kNullValue);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({Value::kEmpty}));
        EXPECT_EQ(res, Value::kEmpty);
    }
    {
        auto result = FunctionManager::get("size", 1);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({true}));
        EXPECT_EQ(res, Value::kNullBadType);
    }
    // current time
    static constexpr std::size_t kCurrentTimeParaNumber = 0;
    // time from literal
    static constexpr std::size_t kLiteralTimeParaNumber = 1;
    // date
    {
        auto result = FunctionManager::get("date", kCurrentTimeParaNumber);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({}));
        EXPECT_EQ(res.type(), Value::Type::DATE);
    }
    {
        auto result = FunctionManager::get("date", kLiteralTimeParaNumber);
        ASSERT_TRUE(result.ok());
        auto res = std::move(result).value()(genArgsRef({true}));
        EXPECT_EQ(res, Value::kNullBadType);
    }
    { TEST_FUNCTION(date, {"2020-09-15"}, Value(Date(2020, 9, 15))); }
    {
        TEST_FUNCTION(
            date, {Map({{"year", 2020}, {"month", 12}, {"day", 31}})}, Value(Date(2020, 12, 31)));
    }
    // leap year February days
    {
        // 2020 is leap
        TEST_FUNCTION(
            date, {Map({{"year", 2020}, {"month", 2}, {"day", 29}})}, Value(Date(2020, 2, 29)));
    }
    {
        // 2021 is not leap
        TEST_FUNCTION(
            date, {Map({{"year", 2021}, {"month", 2}, {"day", 29}})}, Value::kNullBadData);
    }
    // month different days
    {
        TEST_FUNCTION(
            date, {Map({{"year", 2021}, {"month", 1}, {"day", 31}})}, Value(Date(2021, 1, 31)));
    }
    {
        TEST_FUNCTION(
            date, {Map({{"year", 2021}, {"month", 4}, {"day", 31}})}, Value::kNullBadData);
    }
    // range [(−32,768, 1, 1), (32,767, 12, 31)]
    {
        TEST_FUNCTION(
            date,
            {Map({{"year", std::numeric_limits<int16_t>::min()}, {"month", 1}, {"day", 1}})},
            Value(Date(std::numeric_limits<int16_t>::min(), 1, 1)));
    }
    {
        TEST_FUNCTION(
            date,
            {Map({{"year", std::numeric_limits<int16_t>::max()}, {"month", 12}, {"day", 31}})},
            Value(Date(std::numeric_limits<int16_t>::max(), 12, 31)));
    }
    // year
    {
        TEST_FUNCTION(
            date, {Map({{"year", -32769}, {"month", 12}, {"day", 15}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            date, {Map({{"year", 32768}, {"month", 12}, {"day", 31}})}, Value::kNullBadData);
    }
    // month
    {
        TEST_FUNCTION(
            date, {Map({{"year", -32768}, {"month", 13}, {"day", 15}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            date, {Map({{"year", 32767}, {"month", 0}, {"day", 31}})}, Value::kNullBadData);
    }
    // day
    {
        TEST_FUNCTION(
            date, {Map({{"year", -32768}, {"month", 11}, {"day", 0}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            date, {Map({{"year", 32767}, {"month", 1}, {"day", 32}})}, Value::kNullBadData);
    }
    // time
    { TEST_FUNCTION(time, {}, Value::Type::TIME); }
    { TEST_FUNCTION(time, {true}, Value::kNullBadType); }
    { TEST_FUNCTION(time, {"20:09:15"}, Value(time::TimeUtils::timeToUTC(Time(20, 9, 15, 0)))); }
    {
        TEST_FUNCTION(time,
                      {Map({{"hour", 20}, {"minute", 9}, {"second", 15}})},
                      Value(time::TimeUtils::timeToUTC(Time(20, 9, 15, 0))));
    }
    // range [(0, 0, 0, 0), (23, 59, 59, 999999)]
    {
        TEST_FUNCTION(time,
                      {Map({{"hour", 0}, {"minute", 0}, {"second", 0}})},
                      Value(time::TimeUtils::timeToUTC(Time(0, 0, 0, 0))));
    }
    {
        TEST_FUNCTION(time,
                      {Map({{"hour", 23}, {"minute", 59}, {"second", 59}})},
                      Value(time::TimeUtils::timeToUTC(Time(23, 59, 59, 0))));
    }
    // hour
    {
        TEST_FUNCTION(
            time, {Map({{"hour", -1}, {"minute", 9}, {"second", 15}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            time, {Map({{"hour", 24}, {"minute", 9}, {"second", 15}})}, Value::kNullBadData);
    }
    // minute
    {
        TEST_FUNCTION(
            time, {Map({{"hour", 23}, {"minute", -1}, {"second", 15}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            time, {Map({{"hour", 23}, {"minute", 60}, {"second", 15}})}, Value::kNullBadData);
    }
    // second
    {
        TEST_FUNCTION(
            time, {Map({{"hour", 23}, {"minute", 59}, {"second", -1}})}, Value::kNullBadData);
    }
    {
        TEST_FUNCTION(
            time, {Map({{"hour", 23}, {"minute", 59}, {"second", 60}})}, Value::kNullBadData);
    }
    // datetime
    { TEST_FUNCTION(datetime, {}, Value::Type::DATETIME); }
    { TEST_FUNCTION(datetime, {true}, Value::kNullBadType); }
    {
        TEST_FUNCTION(datetime,
                      {"2020-09-15T20:09:15"},
                      Value(time::TimeUtils::dateTimeToUTC(DateTime(2020, 9, 15, 20, 9, 15, 0))));
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 2020},
                            {"month", 9},
                            {"day", 15},
                            {"hour", 20},
                            {"minute", 9},
                            {"second", 15}})},
                      Value(time::TimeUtils::dateTimeToUTC(DateTime(2020, 9, 15, 20, 9, 15, 0))));
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 2020},
                            {"month", 9},
                            {"day", 15},
                            {"hour", 20},
                            {"minute", 9},
                            {"second", 15}})},
                      Value(time::TimeUtils::dateTimeToUTC(DateTime(2020, 9, 15, 20, 9, 15, 0))));
    }
    // range [(−32,768, 1, 1, 0, 0, 0, 0), (32,767, 12, 31, 23, 59, 59, 999999)]
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", -32768},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 0},
                            {"minute", 0},
                            {"second", 0}})},
                      Value(time::TimeUtils::dateTimeToUTC(DateTime(-32768, 1, 1, 0, 0, 0, 0))));
    }
    {
        TEST_FUNCTION(
            datetime,
            {Map({{"year", 32767},
                  {"month", 12},
                  {"day", 31},
                  {"hour", 23},
                  {"minute", 59},
                  {"second", 59}})},
            Value(time::TimeUtils::dateTimeToUTC(DateTime(32767, 12, 31, 23, 59, 59, 0))));
    }
    // year
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 3276700},
                            {"month", 12},
                            {"day", 31},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", -3276700},
                            {"month", 12},
                            {"day", 31},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    // month
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 13},
                            {"day", 31},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 0},
                            {"day", 31},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    // day
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 32},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 0},
                            {"hour", 23},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    // hour
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 24},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", -1},
                            {"minute", 59},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    // minute
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 1},
                            {"minute", 60},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 1},
                            {"minute", -1},
                            {"second", 59}})},
                      Value::kNullBadData);
    }
    // second
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 1},
                            {"minute", 1},
                            {"second", -1}})},
                      Value::kNullBadData);
    }
    {
        TEST_FUNCTION(datetime,
                      {Map({{"year", 32767},
                            {"month", 1},
                            {"day", 1},
                            {"hour", 1},
                            {"minute", 1},
                            {"second", 60}})},
                      Value::kNullBadData);
    }
    // timestamp
    { TEST_FUNCTION(timestamp, {"2020-10-10T10:00:00"}, 1602324000); }
    { TEST_FUNCTION(timestamp, {}, Value::Type::INT); }
    {
        TEST_FUNCTION(e, args_["empty"], M_E);
        TEST_FUNCTION(pi, args_["empty"], M_PI);
        TEST_FUNCTION(radians, args_["radians"], M_PI);
        TEST_FUNCTION(radians, args_["nullvalue"], Value::kNullBadType);
    }
}

TEST_F(FunctionManagerTest, returnType) {
    {
        auto result = FunctionManager::getReturnType("abs", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("bit_and", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("bit_or", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("bit_xor", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("abs", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("abs", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("now", {});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("now", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("rand32", {});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("rand32", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("rand32", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("rand32", {Value::Type::INT, Value::Type::FLOAT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("sqrt", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("sqrt", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("ceil", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("ceil", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("floor", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("floor", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("floor", {Value::Type::STRING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("round", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("round", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("cbrt", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("cbrt", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("pow", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("pow", {Value::Type::INT, Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("pow", {Value::Type::FLOAT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result =
            FunctionManager::getReturnType("pow", {Value::Type::FLOAT, Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("cos", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("sin", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("asin", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("acos", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("acos", {Value::Type::STRING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result =
            FunctionManager::getReturnType("hypot", {Value::Type::FLOAT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("hypot", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result =
            FunctionManager::getReturnType("hypot", {Value::Type::BOOL, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("lower", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toLower", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("upper", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("upper", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("toUpper", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toUpper", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("length", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("length", {Value::Type::PATH});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("length", {});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result =
            FunctionManager::getReturnType("length", {Value::Type::STRING, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("length", {Value::Type::FLOAT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("trim", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("ltrim", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType(
            "replace", {Value::Type::STRING, Value::Type::STRING, Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType(
            "replace",
            {Value::Type::STRING, Value::Type::STRING, Value::Type::STRING, Value::Type::STRING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("reverse", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("reverse", {Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result =
            FunctionManager::getReturnType("split", {Value::Type::STRING, Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::LIST);
    }
    {
        auto result =
            FunctionManager::getReturnType("split", {Value::Type::STRING, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType(
            "substring", {Value::Type::STRING, Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result =
            FunctionManager::getReturnType("substring", {Value::Type::STRING, Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toString", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toString", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toString", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toString", {Value::Type::DATE});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("toBoolean", {Value::Type::BOOL});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::BOOL);
    }
    {
        auto result = FunctionManager::getReturnType("toBoolean", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::BOOL);
    }
    {
        auto result = FunctionManager::getReturnType("toFloat", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("toFloat", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("toFloat", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::FLOAT);
    }
    {
        auto result = FunctionManager::getReturnType("toInteger", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("toInteger", {Value::Type::FLOAT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("toInteger", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("strcasecmp",
                                                     {Value::Type::STRING, Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType("strcasecmp", {Value::Type::STRING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType(
            "lpad", {Value::Type::STRING, Value::Type::INT, Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::STRING);
    }
    {
        auto result = FunctionManager::getReturnType(
            "lpad", {Value::Type::STRING, Value::Type::INT, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result =
            FunctionManager::getReturnType("lpad", {Value::Type::STRING, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::INT});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::NULLVALUE});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::__EMPTY__});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::DATE});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result =
            FunctionManager::getReturnType("hash", {Value::Type::STRING, Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("hash", {});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("hash", {Value::Type::DATASET});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("noexist", {Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Function `noexist' not defined");
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::STRING});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::LIST});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::MAP});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::SET});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::DATASET});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::INT);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::__EMPTY__});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::__EMPTY__);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::NULLVALUE});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), Value::Type::NULLVALUE);
    }
    {
        auto result = FunctionManager::getReturnType("size", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    // time
    {
        auto result = FunctionManager::getReturnType("time", {Value::Type::BOOL});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("time", {});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::TIME);
    }
    {
        auto result = FunctionManager::getReturnType("time", {Value::Type::STRING});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::TIME);
    }
    {
        auto result = FunctionManager::getReturnType("time", {Value::Type::MAP});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::TIME);
    }
    // date
    {
        auto result = FunctionManager::getReturnType("date", {Value::Type::INT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("date", {});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATE);
    }
    {
        auto result = FunctionManager::getReturnType("date", {Value::Type::STRING});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATE);
    }
    {
        auto result = FunctionManager::getReturnType("date", {Value::Type::MAP});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATE);
    }
    // datetime
    {
        auto result = FunctionManager::getReturnType("datetime", {Value::Type::FLOAT});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.status().toString(), "Parameter's type error");
    }
    {
        auto result = FunctionManager::getReturnType("datetime", {});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATETIME);
    }
    {
        auto result = FunctionManager::getReturnType("datetime", {Value::Type::STRING});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATETIME);
    }
    {
        auto result = FunctionManager::getReturnType("datetime", {Value::Type::MAP});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(result.value(), Value::Type::DATETIME);
    }
    {
        auto result = FunctionManager::getReturnType("tags", {Value::Type::VERTEX});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("labels", {Value::Type::VERTEX});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("properties", {Value::Type::VERTEX});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::MAP, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("properties", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::MAP, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("properties", {Value::Type::MAP});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::MAP, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("type", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::STRING, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("typeid", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::INT, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("rank", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::INT, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("startNode", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::VERTEX, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("startNode", {Value::Type::PATH});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::VERTEX, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("startNode", {Value::Type::NULLVALUE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::NULLVALUE, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("endNode", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::VERTEX, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("endNode", {Value::Type::PATH});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::VERTEX, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("endNode", {Value::Type::NULLVALUE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::NULLVALUE, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("keys", {Value::Type::VERTEX});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("keys", {Value::Type::EDGE});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("keys", {Value::Type::MAP});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("nodes", {Value::Type::PATH});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("reverse", {Value::Type::LIST});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("tail", {Value::Type::LIST});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("relationships", {Value::Type::PATH});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("head", {Value::Type::LIST});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::__EMPTY__, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("last", {Value::Type::LIST});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::__EMPTY__, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("coalesce", {Value::Type::LIST});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::__EMPTY__, result.value());
    }
    {
        auto result = FunctionManager::getReturnType("range", {Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
    {
        auto result = FunctionManager::getReturnType(
            "range", {Value::Type::INT, Value::Type::INT, Value::Type::INT});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(Value::Type::LIST, result.value());
    }
}

TEST_F(FunctionManagerTest, SchemaReleated) {
    Vertex vertex;
    Edge edge;

    vertex.vid = "vid";
    vertex.tags.resize(2);
    vertex.tags[0].name = "tag1";
    vertex.tags[0].props = {
        {"p1", 123},
        {"p2", "123"},
        {"p3", true},
        {"p4", false},
    };
    vertex.tags[1].name = "tag2";
    vertex.tags[1].props = {
        {"p1", 456},
        {"p5", "123"},
        {"p6", true},
        {"p7", false},
    };

    edge.src = "src";
    edge.dst = "dst";
    edge.type = 1;
    edge.name = "type";
    edge.ranking = 123;
    edge.props = {
        {"p1", 123},
        {"p2", 456},
        {"p3", true},
        {"p4", false},
    };

    Vertex vertex1;
    Edge edge1;

    vertex1.vid = 0;
    vertex1.tags.resize(1);
    vertex1.tags[0].name = "tag1";
    vertex1.tags[0].props = {
        {"p1", 123},
        {"p2", "123"},
        {"p3", true},
        {"p4", false},
    };
    edge1.src = 0;
    edge1.dst = 1;
    edge1.type = 1;
    edge1.name = "type";
    edge1.ranking = 123;
    edge1.props = {
        {"p1", 123},
        {"p2", 456},
        {"p3", true},
        {"p4", false},
    };

    Edge edge2;
    edge2.src = 0;
    edge2.dst = 1;
    edge2.type = -1;
    edge2.name = "type";
    edge2.ranking = 123;
    edge2.props = {
        {"p1", 123},
        {"p2", 456},
        {"p3", true},
        {"p4", false},
    };

#define TEST_SCHEMA_FUNCTION(fun, arg, expected)                                                   \
    do {                                                                                           \
        auto result = FunctionManager::get(fun, 1);                                                \
        ASSERT_TRUE(result.ok()) << result.status();                                               \
        EXPECT_EQ(expected, result.value()(genArgsRef({arg})));                                    \
    } while (false)

    TEST_SCHEMA_FUNCTION("id", vertex, "vid");
    TEST_SCHEMA_FUNCTION("tags", vertex, Value(List({"tag1", "tag2"})));
    TEST_SCHEMA_FUNCTION("labels", vertex, Value(List({"tag1", "tag2"})));
    TEST_SCHEMA_FUNCTION("properties",
                         vertex,
                         Value(Map({
                             {"p1", 123},
                             {"p2", "123"},
                             {"p3", true},
                             {"p4", false},
                             {"p5", "123"},
                             {"p6", true},
                             {"p7", false},
                         })));
    TEST_SCHEMA_FUNCTION("type", edge, Value("type"));
    TEST_SCHEMA_FUNCTION("typeid", edge, Value(1));
    TEST_SCHEMA_FUNCTION("src", edge, Value("src"));
    TEST_SCHEMA_FUNCTION("dst", edge, Value("dst"));
    TEST_SCHEMA_FUNCTION("rank", edge, Value(123));
    TEST_SCHEMA_FUNCTION("properties",
                         edge,
                         Value(Map({
                             {"p1", 123},
                             {"p2", 456},
                             {"p3", true},
                             {"p4", false},
                         })));
    TEST_SCHEMA_FUNCTION("id", vertex1, 0);
    TEST_SCHEMA_FUNCTION("tags", vertex1, Value(List({"tag1"})));
    TEST_SCHEMA_FUNCTION("labels", vertex1, Value(List({"tag1"})));
    TEST_SCHEMA_FUNCTION("src", edge1, 0);
    TEST_SCHEMA_FUNCTION("dst", edge1, 1);
    TEST_SCHEMA_FUNCTION("rank", edge1, Value(123));
    TEST_SCHEMA_FUNCTION("properties",
                         edge1,
                         Value(Map({
                             {"p1", 123},
                             {"p2", 456},
                             {"p3", true},
                             {"p4", false},
                         })));
    // Reverse edge
    TEST_SCHEMA_FUNCTION("src", edge2, 1);
    TEST_SCHEMA_FUNCTION("dst", edge2, 0);
#undef TEST_SCHEMA_FUNCTION
}

TEST_F(FunctionManagerTest, ScalarFunctionTest) {
    {
        // startNode(null) 、endNode(null) return null
        TEST_FUNCTION(startNode, args_["nullvalue"], Value::kNullValue);
        TEST_FUNCTION(endNode, args_["nullvalue"], Value::kNullValue);
        // startNode(edge) endNode(edge)
        auto start = Vertex("1", {});
        auto end = Vertex("2", {});

        TEST_FUNCTION(startNode, args_["edge"], start);
        TEST_FUNCTION(endNode, args_["edge"], end);
        // startNode(path) endNode(path)
        Path path;
        path.src = Vertex("0", {});
        std::vector<Value> args = {path};
        TEST_FUNCTION(startNode, args, Vertex("0", {}));
        TEST_FUNCTION(endNode, args, Vertex("0", {}));

        path.steps.emplace_back(Step(Vertex("1", {}), 1, "like", 0, {}));
        args[0] = path;
        TEST_FUNCTION(startNode, args, Vertex("0", {}));
        TEST_FUNCTION(endNode, args, Vertex("1", {}));

        path.steps.emplace_back(Step(Vertex("2", {}), 1, "like", 0, {}));
        args[0] = path;
        TEST_FUNCTION(startNode, args, Vertex("0", {}));
        TEST_FUNCTION(endNode, args, Vertex("2", {}));
    }
    {
        // head(null)、 last(null)、coalesce(null) return null
        TEST_FUNCTION(head, args_["nullvalue"], Value::kNullValue);
        TEST_FUNCTION(last, args_["nullvalue"], Value::kNullValue);
        TEST_FUNCTION(coalesce, args_["nullvalue"], Value::kNullValue);

        std::vector<Value> args;
        List list;
        list.values.emplace_back(Value::kNullValue);
        args.push_back(list);

        TEST_FUNCTION(head, args, Value::kNullValue);
        TEST_FUNCTION(last, args, Value::kNullValue);
        TEST_FUNCTION(coalesce, args, Value::kNullValue);

        list.values.insert(list.values.begin(), "head");
        args[0] = list;
        TEST_FUNCTION(head, args, "head");
        TEST_FUNCTION(last, args, Value::kNullValue);
        TEST_FUNCTION(coalesce, args, "head");

        list.values.emplace_back("last");
        args[0] = list;
        TEST_FUNCTION(head, args, "head")
        TEST_FUNCTION(last, args, "last");
        TEST_FUNCTION(coalesce, args, "head");
    }
    {
        // length(null) return null
        TEST_FUNCTION(length, args_["nullvalue"], Value::kNullValue);

        Path path;
        path.src = Vertex("start", {});
        std::vector<Value> args = {path};
        TEST_FUNCTION(length, args, 0);

        for (auto i = 1; i < 4; ++i) {
            path.addStep(Step(Vertex(folly::to<std::string>(i), {}), 1, "like", 0, {}));
        }
        args[0] = path;
        TEST_FUNCTION(length, args, 3);
    }
}

TEST_F(FunctionManagerTest, ListFunctionTest) {
    {
        // keys(null) return null
        TEST_FUNCTION(keys, args_["nullvalue"], Value::kNullValue);
        // keys(vertex)
        Vertex vertex;
        vertex.vid = "v1";
        vertex.tags.emplace_back(Tag("t1", {{"p1", 1}, {"p2", 2}}));
        vertex.tags.emplace_back(Tag("t2", {{"p3", 3}}));
        vertex.tags.emplace_back(Tag("t3", {{"p1", 4}, {"p4", 6}, {"p5", 5}}));
        std::vector<Value> args = {vertex};
        TEST_FUNCTION(keys, args, List({"p1", "p2", "p3", "p4", "p5"}));
        // keys(edge)
        TEST_FUNCTION(keys, args_["edge"], List({"e1", "e2"}));
        // keys(map)
        Map m({{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}});
        args = {m};
        TEST_FUNCTION(keys, args, List({"k1", "k2", "k3"}));
    }
    {
        // nodes(null) return null
        TEST_FUNCTION(nodes, args_["nullvalue"], Value::kNullValue);
        // nodes(path)
        auto v1 = Vertex("101", {});
        auto v2 = Vertex("102", {});
        auto v3 = Vertex("103", {});
        Path path;
        path.src = v1;
        path.steps.emplace_back(Step(v2, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v3, 1, "like", 0, {}));
        std::vector<Value> args = {path};
        TEST_FUNCTION(nodes, args, List({v1, v2, v3}));
    }
    {
        // reverse([2020, "Tony Parker", NULL 19])
        std::vector<Value> args = {List({2020, "Tony Parker", Value::kNullValue, 19})};
        TEST_FUNCTION(reverse, args, List({19, Value::kNullValue, "Tony Parker", 2020}));
    }
    {
        // tail([1, 3, 5, NULL, 4]
        std::vector<Value> args = {List({1, 3, 5, Value::kNullValue, 4})};
        TEST_FUNCTION(tail, args, List({3, 5, Value::kNullValue, 4}));
    }
    {
        // relationships(null) return null
        TEST_FUNCTION(relationships, args_["nullvalue"], Value::kNullValue);

        Path path;
        path.src = Vertex("0", {});
        std::vector<Value> args = {path};
        List expected;
        TEST_FUNCTION(relationships, args, expected);

        for (auto i = 1; i < 4; ++i) {
            path.steps.emplace_back(Step(
                Vertex(folly::to<std::string>(i), {}), 1, "like", 0, {{"likeness", (i + 50)}}));
        }
        args[0] = path;
        for (auto i = 0; i < 3; ++i) {
            expected.values.emplace_back(Edge(folly::to<std::string>(i),
                                              folly::to<std::string>(i + 1),
                                              1,
                                              "like",
                                              0,
                                              {{"likeness", (i + 51)}}));
        }
        TEST_FUNCTION(relationships, args, expected);
    }
}

TEST_F(FunctionManagerTest, duplicateEdgesORVerticesInPath) {
    {
        Path path = createPath("0", {});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, false);
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        Path path = createPath("0", {"0"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, true);
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        Path path = createPath("0", {"1"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, false);
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        Path path = createPath("0", {"1", "2"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, false);
    }
    {
        Path path = createPath("0", {"1", "2", "1"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, true);
    }
    {
        Path path = createPath("0", {"1", "2", "3", "0"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, true);
    }
    {
        Path path = createPath("0", {"1", "2", "3", "0", "1"});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, true);
        TEST_FUNCTION(hasSameEdgeInPath, args, true);
    }
    {
        auto v0 = Vertex("0", {});
        auto v1 = Vertex("1", {});
        Path path;
        path.src = v0;
        path.steps.emplace_back(Step(v1, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v0, -1, "like", 0, {}));

        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameEdgeInPath, args, true);
    }
    {
        auto v0 = Vertex("0", {});
        auto v1 = Vertex("1", {});
        Path path;
        path.src = v0;
        path.steps.emplace_back(Step(v1, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v0, 1, "like", 0, {}));

        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        auto v0 = Vertex("0", {});
        auto v1 = Vertex("1", {});
        Path path;
        path.src = v0;
        path.steps.emplace_back(Step(v1, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v0, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v1, 1, "like", 1, {}));

        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        Path path = createPath(0, {1});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, false);
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
    {
        Path path = createPath(0, {1, 2, 3, 0, 1});
        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameVertexInPath, args, true);
        TEST_FUNCTION(hasSameEdgeInPath, args, true);
    }
    {
        auto v0 = Vertex(0, {});
        auto v1 = Vertex(1, {});
        Path path;
        path.src = v0;
        path.steps.emplace_back(Step(v1, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v0, -1, "like", 0, {}));

        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameEdgeInPath, args, true);
    }
    {
        auto v0 = Vertex(0, {});
        auto v1 = Vertex(1, {});
        Path path;
        path.src = v0;
        path.steps.emplace_back(Step(v1, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v0, 1, "like", 0, {}));

        std::vector<Value> args = {path};
        TEST_FUNCTION(hasSameEdgeInPath, args, false);
    }
}

TEST_F(FunctionManagerTest, ReversePath) {
    {
        Path path = createPath("0", {"1", "2", "3"});
        std::vector<Value> args = {path};
        Path expected;
        expected.src = Vertex("3", {});
        expected.steps.emplace_back(Step(Vertex("2", {}), -1, "edge1", 0, {}));
        expected.steps.emplace_back(Step(Vertex("1", {}), -1, "edge1", 0, {}));
        expected.steps.emplace_back(Step(Vertex("0", {}), -1, "edge1", 0, {}));
        TEST_FUNCTION(reversePath, args, expected);
    }
}

TEST_F(FunctionManagerTest, DataSetRowCol) {
    auto dataset = DataSet({"col0", "col1", "col2"});
    dataset.emplace_back(Row({1, true, "233"}));
    dataset.emplace_back(Row({4, false, "456"}));
    Value datasetValue = Value(std::move(dataset));
    // out of range
    {
        TEST_FUNCTION(dataSetRowCol,
                      std::vector<Value>({datasetValue, Value(-1), Value(2)}),
                      Value::kNullBadData);
        TEST_FUNCTION(dataSetRowCol,
                      std::vector<Value>({datasetValue, Value(4), Value(2)}),
                      Value::kNullBadData);
        TEST_FUNCTION(dataSetRowCol,
                      std::vector<Value>({datasetValue, Value(0), Value(-1)}),
                      Value::kNullBadData);
        TEST_FUNCTION(dataSetRowCol,
                      std::vector<Value>({datasetValue, Value(0), Value(3)}),
                      Value::kNullBadData);
    }
    // ok
    {
        TEST_FUNCTION(
            dataSetRowCol, std::vector<Value>({datasetValue, Value(0), Value(0)}), Value(1));
        TEST_FUNCTION(
            dataSetRowCol, std::vector<Value>({datasetValue, Value(1), Value(2)}), Value("456"));
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    auto result = nebula::time::Timezone::initializeGlobalTimezone();
    if (!result.ok()) {
        LOG(FATAL) << result;
    }

    DLOG(INFO) << "Timezone: " << nebula::time::Timezone::getGlobalTimezone().stdZoneName();
    DLOG(INFO) << "Timezone offset: "
               << nebula::time::Timezone::getGlobalTimezone().utcOffsetSecs();

    return RUN_ALL_TESTS();
}
