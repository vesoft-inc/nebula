/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"
#include "common/function/FunctionManager.h"

namespace nebula {

class InnerproductTest : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 protected:
  std::vector<FunctionManager::ArgType> genArgsRef(const std::vector<Value>& args) {
    std::vector<FunctionManager::ArgType> argsRef;
    argsRef.insert(argsRef.end(), args.begin(), args.end());
    return argsRef;
  }
};

TEST_F(InnerproductTest, BasicInnerProductCalculation) {
  {
    // Test basic inner product: [1.0, 2.0] and [3.0, 4.0] -> 1*3 + 2*4 = 11.0
    auto vec1 = Vector({1.0f, 2.0f});
    auto vec2 = Vector({3.0f, 4.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(11.0, res.getFloat());
  }
  {
    // Test zero inner product: orthogonal vectors [1.0, 0.0] and [0.0, 1.0] -> 0.0
    auto vec1 = Vector({1.0f, 0.0f});
    auto vec2 = Vector({0.0f, 1.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(0.0, res.getFloat());
  }
  {
    // Test same vector inner product: [2.0, 3.0] with itself -> 2*2 + 3*3 = 13.0
    auto vec1 = Vector({2.0f, 3.0f});
    auto vec2 = Vector({2.0f, 3.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(13.0, res.getFloat());
  }
  {
    // Test higher dimensions: [1,2,3] and [4,5,6] -> 1*4 + 2*5 + 3*6 = 32.0
    auto vec1 = Vector({1.0f, 2.0f, 3.0f});
    auto vec2 = Vector({4.0f, 5.0f, 6.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(32.0, res.getFloat());
  }
  {
    // Test negative values: [-1.0, 2.0] and [3.0, -4.0] -> -1*3 + 2*(-4) = -11.0
    auto vec1 = Vector({-1.0f, 2.0f});
    auto vec2 = Vector({3.0f, -4.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(-11.0, res.getFloat());
  }
}

TEST_F(InnerproductTest, ErrorCases) {
  {
    // Test dimension mismatch: 2D vs 3D vector
    auto vec1 = Vector({1.0f, 2.0f});
    auto vec2 = Vector({4.0f, 6.0f, 8.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::NULLVALUE);
  }
  {
    // Test empty vectors
    auto vec1 = Vector(std::vector<float>{});
    auto vec2 = Vector(std::vector<float>{});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(0.0, res.getFloat());
  }
  {
    // Test non-vector types: should return kNullBadType
    std::vector<Value> args = {Value(1), Value(2)};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::NULLVALUE);
  }
  {
    // Test mixed types: vector and non-vector
    auto vec1 = Vector({1.0f, 2.0f});
    std::vector<Value> args = {vec1, Value(5)};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::NULLVALUE);
  }
}

TEST_F(InnerproductTest, TypeSignatureTest) {
  {
    // Test correct type signature: Vector, Vector -> Float
    auto result =
        FunctionManager::getReturnType("innerproduct", {Value::Type::VECTOR, Value::Type::VECTOR});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(result.value(), Value::Type::FLOAT);
  }
  {
    // Test incorrect type signature: wrong argument types
    auto result =
        FunctionManager::getReturnType("innerproduct", {Value::Type::INT, Value::Type::INT});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "Parameter's type error");
  }
  {
    // Test incorrect type signature: wrong number of arguments
    auto result = FunctionManager::getReturnType("innerproduct", {Value::Type::VECTOR});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "Parameter's type error");
  }
  {
    // Test incorrect type signature: too many arguments
    auto result = FunctionManager::getReturnType(
        "innerproduct", {Value::Type::VECTOR, Value::Type::VECTOR, Value::Type::VECTOR});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "Parameter's type error");
  }
}

TEST_F(InnerproductTest, SpecialCases) {
  {
    // Test with zero vectors: [0.0, 0.0] and [1.0, 2.0] -> 0.0
    auto vec1 = Vector({0.0f, 0.0f});
    auto vec2 = Vector({1.0f, 2.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(0.0, res.getFloat());
  }
  {
    // Test with very small numbers: precision test
    auto vec1 = Vector({0.001f, 0.002f});
    auto vec2 = Vector({0.003f, 0.004f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    // 0.001 * 0.003 + 0.002 * 0.004 = 0.000003 + 0.000008 = 0.000011
    EXPECT_NEAR(0.000011, res.getFloat(), 1e-8);
  }
  {
    // Test with large numbers
    auto vec1 = Vector({1000.0f, 2000.0f});
    auto vec2 = Vector({3000.0f, 4000.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("innerproduct", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    // 1000 * 3000 + 2000 * 4000 = 3000000 + 8000000 = 11000000
    EXPECT_DOUBLE_EQ(11000000.0, res.getFloat());
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
