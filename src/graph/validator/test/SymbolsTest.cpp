/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "graph/validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class SymbolsTest : public ValidatorTestBase {};

static ::testing::AssertionResult checkNodes(std::unordered_set<PlanNode*> nodes,
                                             std::unordered_set<int64_t> expected) {
  if (nodes.size() != expected.size()) {
    std::stringstream ss;
    for (auto node : nodes) {
      ss << node->id() << ",";
    }

    ss << " vs. ";
    for (auto id : expected) {
      ss << id << ",";
    }
    return ::testing::AssertionFailure() << "size not match, " << ss.str();
  }

  if (nodes.empty() && expected.empty()) {
    return ::testing::AssertionSuccess();
  }

  for (auto node : nodes) {
    if (expected.find(node->id()) == expected.end()) {
      return ::testing::AssertionFailure() << node->id() << " not find.";
    }
  }

  return ::testing::AssertionSuccess();
}

TEST_F(SymbolsTest, Variables) {
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 2 STEPS FROM $-.id OVER like YIELD like._dst";
    auto status = validate(query);
    EXPECT_TRUE(status.ok());
    auto qctx = std::move(status).value();
    EXPECT_NE(qctx, nullptr);
    auto* symTable = qctx->symTable();

    {
      auto varName = "__Start_1";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(variable->colNames.empty());
      EXPECT_TRUE(checkNodes(variable->readBy, {}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {1}));
    }
    {
      auto varName = "__Expand_2";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(checkNodes(variable->readBy, {3}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {2}));
    }
    {
      auto varName = "__ExpandAll_3";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(checkNodes(variable->readBy, {4}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {3}));
    }
    {
      auto varName = "__Project_4";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_EQ(variable->colNames, std::vector<std::string>({"id"}));
      EXPECT_TRUE(checkNodes(variable->readBy, {9}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {4}));
    }
    {
      auto varName = "__Argument_6";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_EQ(variable->colNames, std::vector<std::string>({"id"}));
      EXPECT_TRUE(checkNodes(variable->readBy, {7}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {6}));
    }
    {
      auto varName = "__Expand_7";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_EQ(variable->colNames, std::vector<std::string>({"_expand_vid", "_expand_dst"}));
      EXPECT_TRUE(checkNodes(variable->readBy, {8}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {7}));
    }
    {
      auto varName = "__ExpandAll_8";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(checkNodes(variable->readBy, {9}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {8}));
    }
    {
      auto varName = "__HashInnerJoin_9";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(checkNodes(variable->readBy, {10}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {9}));
    }
    {
      auto varName = "__Project_10";
      auto* variable = symTable->getVar(varName);
      EXPECT_NE(variable, nullptr);
      EXPECT_EQ(variable->name, varName);
      EXPECT_EQ(variable->type, Value::Type::DATASET);
      EXPECT_TRUE(checkNodes(variable->readBy, {}));
      EXPECT_TRUE(checkNodes(variable->writtenBy, {10}));
    }
  }
}
}  // namespace graph
}  // namespace nebula
