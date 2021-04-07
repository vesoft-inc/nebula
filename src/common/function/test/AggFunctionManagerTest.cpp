/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <gtest/gtest.h>
#include "common/function/AggFunctionManager.h"
#include "common/datatypes/Set.h"
#include "common/time/TimeUtils.h"
#include "common/datatypes/List.h"

namespace nebula {

class AggFunctionManagerTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    void testFunction(const char *expr, const std::vector<Value> &groupData, Value expect) {
        auto result = AggFunctionManager::get(expr);
        ASSERT_TRUE(result.ok());
        auto aggFunc = result.value();
        AggData aggData;
        for (auto i : groupData) {
            aggFunc(&aggData, i);
        }
        auto res = aggData.result();
        EXPECT_EQ(res.type(), expect.type()) << "agg function return type check failed: " << expr;
        EXPECT_EQ(res, expect) << "agg function return value check failed: " << expr;
    }

    static std::unordered_map<std::string, std::vector<Value>> testData_;
};

std::unordered_map<std::string, std::vector<Value>> AggFunctionManagerTest::testData_ = {
    {"empty", {Value()}},
    {"null", {NullType::__NULL__}},
    {"int", {1, 2, 3}},
    {"float", {1.1, 2.2, 3.3}},
    {"mixed", {1, NullType::__NULL__, 2.0, Value()}},
};

#define TEST_FUNCTION(expr, args, expected)                                                        \
    do {                                                                                           \
        testFunction(#expr, args, expected);                                                       \
    } while (0);

TEST_F(AggFunctionManagerTest, aggFunc) {
    {
        TEST_FUNCTION(count, testData_["empty"], 0);
        TEST_FUNCTION(count, testData_["null"], 0);
        TEST_FUNCTION(count, testData_["int"], 3);
        TEST_FUNCTION(count, testData_["float"], 3);
        TEST_FUNCTION(count, testData_["mixed"], 2);
    }
    {
        TEST_FUNCTION(sum, testData_["empty"], 0);
        TEST_FUNCTION(sum, testData_["null"], 0);
        TEST_FUNCTION(sum, testData_["int"], 6);
        TEST_FUNCTION(sum, testData_["float"], 6.6);
        TEST_FUNCTION(sum, testData_["mixed"], 3.0);
    }
    {
        TEST_FUNCTION(min, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(min, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(min, testData_["int"], 1);
        TEST_FUNCTION(min, testData_["float"], 1.1);
        TEST_FUNCTION(min, testData_["mixed"], 1);
    }
    {
        TEST_FUNCTION(max, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(max, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(max, testData_["int"], 3);
        TEST_FUNCTION(max, testData_["float"], 3.3);
        TEST_FUNCTION(max, testData_["mixed"], 2.0);
    }
    {
        TEST_FUNCTION(avg, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(avg, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(avg, testData_["int"], 2.0);
        TEST_FUNCTION(avg, testData_["float"], 2.2);
        TEST_FUNCTION(avg, testData_["mixed"], 1.5);
    }
    {
        TEST_FUNCTION(std, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(std, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(std, testData_["int"], 0.816496580927726);
        TEST_FUNCTION(std, testData_["float"], 0.8981462390204984);
        TEST_FUNCTION(std, testData_["mixed"], 0.5);
    }
    {
        TEST_FUNCTION(bit_and, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(bit_and, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(bit_and, testData_["int"], 0);
        TEST_FUNCTION(bit_and, testData_["float"], Value::kNullValue);
        TEST_FUNCTION(bit_and, testData_["mixed"], Value::kNullValue);
    }
    {
        TEST_FUNCTION(bit_or, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(bit_or, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(bit_or, testData_["int"], 3);
        TEST_FUNCTION(bit_or, testData_["float"], Value::kNullValue);
        TEST_FUNCTION(bit_or, testData_["mixed"], Value::kNullValue);
    }
    {
        TEST_FUNCTION(bit_xor, testData_["empty"], Value::kNullValue);
        TEST_FUNCTION(bit_xor, testData_["null"], Value::kNullValue);
        TEST_FUNCTION(bit_xor, testData_["int"], 0);
        TEST_FUNCTION(bit_xor, testData_["float"], Value::kNullValue);
        TEST_FUNCTION(bit_xor, testData_["mixed"], Value::kNullValue);
    }
    {
        TEST_FUNCTION(collect, testData_["empty"], List());
        TEST_FUNCTION(collect, testData_["null"], List());
        TEST_FUNCTION(collect, testData_["int"], List({1, 2, 3}));
        TEST_FUNCTION(collect, testData_["float"], List({1.1, 2.2, 3.3}));
        TEST_FUNCTION(collect, testData_["mixed"], List({1, 2.0}));
    }
    {
        TEST_FUNCTION(collect_set, testData_["empty"], Set());
        TEST_FUNCTION(collect_set, testData_["null"], Set());
        TEST_FUNCTION(collect_set, testData_["int"], Set({1, 2, 3}));
        TEST_FUNCTION(collect_set, testData_["float"], Set({1.1, 2.2, 3.3}));
        TEST_FUNCTION(collect_set, testData_["mixed"], Set({1, 2.0}));
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
