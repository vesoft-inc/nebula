/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>
#include <ostream>
#include "common/expression/test/TestBase.h"

#define TEST_AGG(name, isDistinct, expr, inputVar, expected)                                       \
    do {                                                                                           \
        EXPECT_TRUE(testAggExpr(#name, isDistinct, #expr, inputVar, expected));                    \
    } while (0)

namespace nebula {

template<typename K, typename V>
static inline std::ostream& operator<<(std::ostream &os, const std::unordered_map<K, V> &m) {
    os << "{";
    for (const auto &i : m) {
        os << i.first << ":" << i.second << ",";
    }
    os << "}";
    return os;
}

class AggregateExpressionTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    ::testing::AssertionResult testAggExpr(const char* name,
                                           bool isDistinct,
                                           const char* expr,
                                           std::vector<std::pair<std::string, Value>> inputVar,
                                           const std::unordered_map<std::string, Value>& expected) {
        auto agg = name;
        auto func = std::make_unique<std::string>(expr);
        Expression* arg = nullptr;
        auto isConst = false;
        if (!func->compare("isConst")) {
            isConst = true;
            arg = ConstantExpression::make(&pool);
        }
        auto aggExpr = AggregateExpression::make(&pool, agg, arg, isDistinct);
        std::unordered_map<std::string, std::unique_ptr<AggData>> agg_data_map;
        for (const auto& row : inputVar) {
            auto iter = agg_data_map.find(row.first);
            if (iter == agg_data_map.end()) {
                agg_data_map[row.first] = std::make_unique<AggData>();
            }
            if (isConst) {
                static_cast<ConstantExpression*>(arg)->setValue(row.second);
            } else {
                auto args = ArgumentList::make(&pool, 1);
                args->addArgument(ConstantExpression::make(&pool, row.second));
                aggExpr->setArg(FunctionCallExpression::make(&pool, *func, args));
            }
            aggExpr->setAggData(agg_data_map[row.first].get());
            auto eval = aggExpr->eval(gExpCtxt);
        }
        std::unordered_map<std::string, Value> res;
        for (auto& iter : agg_data_map) {
            res[iter.first] = iter.second->result();
        }
        if (res != expected) {
            return ::testing::AssertionFailure() << "Expect: " << expected << ", Got: " << res;
        } else {
            return ::testing::AssertionSuccess();
        }
    }
};

TEST_F(AggregateExpressionTest, AggregateExpression) {
    std::vector<std::pair<std::string, Value>> vals1_ = {
        {"a", 1}, {"b", 4}, {"c", 3}, {"a", 3}, {"c", 8}, {"c", 5}, {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals2_ = {{"a", 1},
                                                         {"b", 4},
                                                         {"c", Value::kNullValue},
                                                         {"c", 3},
                                                         {"a", 3},
                                                         {"a", Value::kEmpty},
                                                         {"b", Value::kEmpty},
                                                         {"c", Value::kEmpty},
                                                         {"c", 8},
                                                         {"a", Value::kNullValue},
                                                         {"c", 5},
                                                         {"b", Value::kNullValue},
                                                         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals3_ = {
        {"a", 1}, {"b", 4}, {"c", 3}, {"a", 3}, {"c", 8}, {"c", 5}, {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals4_ = {{"a", 1},
                                                         {"b", 4},
                                                         {"c", 3},
                                                         {"c", Value::kNullValue},
                                                         {"a", Value::kEmpty},
                                                         {"b", Value::kEmpty},
                                                         {"c", Value::kEmpty},
                                                         {"a", Value::kNullValue},
                                                         {"b", Value::kNullValue},
                                                         {"a", 3},
                                                         {"c", 8},
                                                         {"c", 5},
                                                         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals5_ = {{"c", Value::kNullValue},
                                                         {"a", Value::kEmpty},
                                                         {"b", Value::kEmpty},
                                                         {"c", Value::kEmpty},
                                                         {"a", Value::kNullValue},
                                                         {"b", Value::kNullValue}};

    std::vector<std::pair<std::string, Value>> vals6_ = {{"a", true},
                                                         {"b", false},
                                                         {"c", true},
                                                         {"a", false},
                                                         {"c", true},
                                                         {"c", false},
                                                         {"c", true}};

    std::vector<std::pair<std::string, Value>> vals7_ = {{"a", 0},
                                                         {"a", 1},
                                                         {"a", 2},
                                                         {"a", 3},
                                                         {"a", 4},
                                                         {"a", 5},
                                                         {"a", 6},
                                                         {"a", 7},
                                                         {"b", 6},
                                                         {"c", 7},
                                                         {"c", 7},
                                                         {"a", 8},
                                                         {"c", 9},
                                                         {"c", 9},
                                                         {"a", 9}};

    std::vector<std::pair<std::string, Value>> vals8_ = {{"a", 0},
                                                         {"a", 1},
                                                         {"a", 2},
                                                         {"c", Value::kEmpty},
                                                         {"a", Value::kEmpty},
                                                         {"a", 3},
                                                         {"a", 4},
                                                         {"a", 5},
                                                         {"c", Value::kNullValue},
                                                         {"a", 6},
                                                         {"a", 7},
                                                         {"b", Value::kEmpty},
                                                         {"b", 6},
                                                         {"a", Value::kNullValue},
                                                         {"c", 7},
                                                         {"c", 7},
                                                         {"a", 8},
                                                         {"c", 9},
                                                         {"b", Value::kNullValue},
                                                         {"c", 9},
                                                         {"a", 9}};

    std::vector<std::pair<std::string, Value>> vals9_ = {{"a", true},
                                                         {"b", true},
                                                         {"c", false},
                                                         {"a", false},
                                                         {"c", false},
                                                         {"c", false},
                                                         {"c", true}};
    std::vector<std::pair<std::string, Value>> vals10_ = {{"a", "true"},
                                                          {"b", "12"},
                                                          {"c", "a"},
                                                          {"a", "false"},
                                                          {"c", "zxA"},
                                                          {"c", "zxbC"},
                                                          {"c", "Ca"}};

    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 3}, {"b", 4}, {"c", 8}};
        TEST_AGG(, false, abs, vals1_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 3}, {"b", 4}, {"c", 5}};
        TEST_AGG(, true, abs, vals1_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 2}, {"b", 1}, {"c", 4}};
        TEST_AGG(COUNT, false, abs, vals1_, expected1);
        TEST_AGG(COUNT, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 2}, {"b", 1}, {"c", 3}};
        TEST_AGG(COUNT, true, abs, vals1_, expected2);
        TEST_AGG(COUNT, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 4}, {"b", 4}, {"c", 24}};
        TEST_AGG(SUM, false, abs, vals1_, expected1);
        TEST_AGG(SUM, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 4}, {"b", 4}, {"c", 16}};
        TEST_AGG(SUM, true, abs, vals1_, expected2);
        TEST_AGG(SUM, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 2}, {"b", 4}, {"c", 6}};
        TEST_AGG(AVG, false, abs, vals1_, expected1);
        TEST_AGG(AVG, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {
            {"a", 2}, {"b", 4}, {"c", 16.0 / 3}};
        TEST_AGG(AVG, true, abs, vals1_, expected2);
        TEST_AGG(AVG, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 1}, {"b", 4}, {"c", 3}};
        TEST_AGG(MIN, false, abs, vals1_, expected1);
        TEST_AGG(MIN, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 1}, {"b", 4}, {"c", 3}};
        TEST_AGG(MIN, true, abs, vals1_, expected2);
        TEST_AGG(MIN, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 3}, {"b", 4}, {"c", 8}};
        TEST_AGG(MAX, false, abs, vals1_, expected1);
        TEST_AGG(MAX, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 3}, {"b", 4}, {"c", 8}};
        TEST_AGG(MAX, true, abs, vals1_, expected2);
        TEST_AGG(MAX, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {
            {"a", Value(List({1, 3}))}, {"b", Value(List({4}))}, {"c", Value(List({3, 8, 5, 8}))}};
        TEST_AGG(COLLECT, false, abs, vals1_, expected1);
        TEST_AGG(COLLECT, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {
            {"a", List({1, 3})}, {"b", List({4})}, {"c", List({3, 8, 5})}};
        TEST_AGG(COLLECT, true, abs, vals1_, expected2);
        TEST_AGG(COLLECT, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {
            {"b", Set({4})}, {"a", Set({1, 3})}, {"c", Set({3, 8, 5})}};
        TEST_AGG(COLLECT_SET, false, abs, vals1_, expected1);
        TEST_AGG(COLLECT_SET, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {
            {"b", Set({4})}, {"a", Set({1, 3})}, {"c", Set({3, 8, 5})}};
        TEST_AGG(COLLECT_SET, true, abs, vals1_, expected2);
        TEST_AGG(COLLECT_SET, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {
            {"a", 2.8722813232690143}, {"b", 0.0}, {"c", 0.9999999999999999}};
        TEST_AGG(STD, false, abs, vals7_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {
            {"a", 2.8722813232690143}, {"b", 0.0}, {"c", 1.0}};
        TEST_AGG(STD, true, abs, vals7_, expected2);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {{"a", 1}, {"b", 4}, {"c", 0}};
        TEST_AGG(BIT_AND, false, abs, vals3_, expected1);
        TEST_AGG(BIT_AND, false, isConst, vals4_, expected1);

        const std::unordered_map<std::string, Value> expected2 = {{"a", 1}, {"b", 4}, {"c", 0}};
        TEST_AGG(BIT_AND, true, abs, vals3_, expected2);
        TEST_AGG(BIT_AND, true, isConst, vals4_, expected2);

        const std::unordered_map<std::string, Value> expected3 = {{"a", 3}, {"b", 4}, {"c", 15}};
        TEST_AGG(BIT_OR, false, abs, vals3_, expected3);
        TEST_AGG(BIT_OR, false, isConst, vals4_, expected3);

        const std::unordered_map<std::string, Value> expected4 = {{"a", 3}, {"b", 4}, {"c", 15}};
        TEST_AGG(BIT_OR, true, abs, vals3_, expected4);
        TEST_AGG(BIT_OR, true, isConst, vals4_, expected4);

        const std::unordered_map<std::string, Value> expected5 = {{"a", 2}, {"b", 4}, {"c", 6}};
        TEST_AGG(BIT_XOR, false, abs, vals3_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals4_, expected5);

        const std::unordered_map<std::string, Value> expected6 = {{"a", 2}, {"b", 4}, {"c", 14}};
        TEST_AGG(BIT_XOR, true, abs, vals3_, expected6);
        TEST_AGG(BIT_XOR, true, isConst, vals4_, expected6);
    }
    {
        const std::unordered_map<std::string, Value> expected1 = {
            {"a", Value::kNullValue}, {"b", Value::kNullValue}, {"c", Value::kNullValue}};
        const std::unordered_map<std::string, Value> expected2 = {{"a", 0}, {"b", 0}, {"c", 0}};
        const std::unordered_map<std::string, Value> expected3 = {
            {"a", Value(List())}, {"b", Value(List())}, {"c", Value(List())}};
        const std::unordered_map<std::string, Value> expected4 = {
            {"a", Value(Set())}, {"b", Value(Set())}, {"c", Value(Set())}};
        const std::unordered_map<std::string, Value> expected5 = {
            {"a", Value::kNullBadType}, {"b", Value::kNullBadType}, {"c", Value::kNullBadType}};
        const std::unordered_map<std::string, Value> expected6 = {
            {"a", false}, {"b", true}, {"c", false}};
        const std::unordered_map<std::string, Value> expected7 = {
            {"a", true}, {"b", true}, {"c", true}};

        TEST_AGG(COUNT, false, isConst, vals5_, expected2);
        TEST_AGG(COUNT, true, isConst, vals5_, expected2);
        TEST_AGG(COUNT, false, abs, vals9_, expected2);
        TEST_AGG(COUNT, true, abs, vals9_, expected2);
        TEST_AGG(SUM, false, isConst, vals5_, expected2);
        TEST_AGG(SUM, true, isConst, vals5_, expected2);
        TEST_AGG(SUM, false, isConst, vals9_, expected5);
        TEST_AGG(SUM, true, isConst, vals9_, expected5);
        TEST_AGG(AVG, false, isConst, vals5_, expected1);
        TEST_AGG(AVG, true, isConst, vals5_, expected1);
        TEST_AGG(AVG, false, isConst, vals9_, expected5);
        TEST_AGG(AVG, true, isConst, vals9_, expected5);
        TEST_AGG(MAX, false, isConst, vals5_, expected1);
        TEST_AGG(MAX, true, isConst, vals5_, expected1);
        TEST_AGG(MAX, false, isConst, vals9_, expected7);
        TEST_AGG(MAX, true, isConst, vals9_, expected7);
        TEST_AGG(MIN, false, isConst, vals5_, expected1);
        TEST_AGG(MIN, true, isConst, vals5_, expected1);
        TEST_AGG(MIN, false, isConst, vals9_, expected6);
        TEST_AGG(MIN, true, isConst, vals9_, expected6);
        TEST_AGG(STD, false, isConst, vals5_, expected1);
        TEST_AGG(STD, true, isConst, vals5_, expected1);
        TEST_AGG(STD, false, isConst, vals9_, expected5);
        TEST_AGG(STD, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_AND, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_AND, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_AND, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_AND, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_AND, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_AND, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_OR, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_OR, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_OR, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_OR, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_OR, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_OR, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_XOR, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_XOR, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_XOR, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_XOR, true, isConst, vals9_, expected5);
        TEST_AGG(COLLECT, false, isConst, vals5_, expected3);
        TEST_AGG(COLLECT, true, isConst, vals5_, expected3);
        TEST_AGG(COLLECT_SET, false, isConst, vals5_, expected4);
        TEST_AGG(COLLECT_SET, true, isConst, vals5_, expected4);
    }
}

TEST_F(ExpressionTest, AggregateToString) {
    auto arg = ConstantExpression::make(&pool, "$-.age");
    auto* aggName = "COUNT";
    auto expr = AggregateExpression::make(&pool, aggName, arg, true);
    ASSERT_EQ("COUNT(distinct $-.age)", expr->toString());
}

}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
