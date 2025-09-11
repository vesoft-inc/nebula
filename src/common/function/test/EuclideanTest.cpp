/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"
#include "common/function/FunctionManager.h"

namespace nebula {

class EuclideanTest : public ::testing::Test {
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

TEST_F(EuclideanTest, BasicDistanceCalculation) {
  {
    // Test basic euclidean distance: [1.0, 2.0] and [4.0, 6.0] -> sqrt((1-4)^2 + (2-6)^2) = 5.0
    auto vec1 = Vector({1.0f, 2.0f});
    auto vec2 = Vector({4.0f, 6.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("euclidean", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(5.0, res.getFloat());
  }
  {
    // Test zero distance: same vectors
    auto vec1 = Vector({1.0f, 2.0f, 3.0f});
    auto vec2 = Vector({1.0f, 2.0f, 3.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("euclidean", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(0.0, res.getFloat());
  }
  {
    // Test higher dimensions: [0,0,0] and [1,1,1] -> sqrt(3) ≈ 1.732
    auto vec1 = Vector({0.0f, 0.0f, 0.0f});
    auto vec2 = Vector({1.0f, 1.0f, 1.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("euclidean", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(std::sqrt(3.0), res.getFloat());
  }
}

TEST_F(EuclideanTest, ErrorCases) {
  {
    // Test dimension mismatch: 2D vs 3D vector
    auto vec1 = Vector({1.0f, 2.0f});
    auto vec2 = Vector({4.0f, 6.0f, 8.0f});
    std::vector<Value> args = {vec1, vec2};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("euclidean", args.size());
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
    auto result = FunctionManager::get("euclidean", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::FLOAT);
    EXPECT_DOUBLE_EQ(0.0, res.getFloat());
  }
  {
    // Test non-vector types: should return kNullBadType
    std::vector<Value> args = {Value(1), Value(2)};
    auto argsRef = genArgsRef(args);
    auto result = FunctionManager::get("euclidean", args.size());
    ASSERT_TRUE(result.ok());
    auto res = result.value()(argsRef);
    EXPECT_EQ(res.type(), Value::Type::NULLVALUE);
  }
}

TEST_F(EuclideanTest, TypeSignatureTest) {
  {
    // Test correct type signature: Vector, Vector -> Float
    auto result =
        FunctionManager::getReturnType("euclidean", {Value::Type::VECTOR, Value::Type::VECTOR});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(result.value(), Value::Type::FLOAT);
  }
  {
    // Test incorrect type signature: wrong argument types
    auto result = FunctionManager::getReturnType("euclidean", {Value::Type::INT, Value::Type::INT});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "Parameter's type error");
  }
  {
    // Test incorrect type signature: wrong number of arguments
    auto result = FunctionManager::getReturnType("euclidean", {Value::Type::VECTOR});
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().toString(), "Parameter's type error");
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
