/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class SymbolsTest : public ValidatorTestBase {
};

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
        std::string query = "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
                            "id | GO 2 STEPS FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok());
        auto qctx = std::move(status).value();
        EXPECT_NE(qctx, nullptr);
        auto* symTable = qctx->symTable();

        {
            auto varName = "__Start_0";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_TRUE(checkNodes(variable->readBy, {}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {0}));
        }
        {
            auto varName = "__GetNeighbors_1";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_TRUE(checkNodes(variable->readBy, {2}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {1}));
        }
        {
            auto varName = "__Project_2";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {3, 9, 20}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {2}));
        }
        {
            auto varName = "__Project_3";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {4}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {3}));
        }
        {
            auto varName = "__Dedup_4";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {17, 6}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {8, 4}));
        }
        {
            auto varName = "__Project_9";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__COL_1"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {10}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {9}));
        }
        {
            auto varName = "__Dedup_10";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__COL_1"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {13, 16, 19}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {10, 15}));
        }
        {
            auto varName = "__Start_5";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_TRUE(checkNodes(variable->readBy, {}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {5}));
        }
        {
            auto varName = "__GetNeighbors_6";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_TRUE(checkNodes(variable->readBy, {11, 7}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {6}));
        }
        {
            auto varName = "__Project_7";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"_vid"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {8}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {7}));
        }
        {
            auto varName = "__Project_11";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"__COL_2", "TRACK_DST_VID"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {12}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {11}));
        }
        {
            auto varName = "__Dedup_12";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"__COL_2", "TRACK_DST_VID"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {13}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {12}));
        }
        {
            auto varName = "__InnerJoin_13";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>({"id", "__COL_1", "__COL_2", "TRACK_DST_VID"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {14}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {13}));
        }
        {
            auto varName = "__Project_14";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"id", "__COL_1"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {15}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {14}));
        }
        {
            auto varName = "__Loop_16";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({}));
            EXPECT_TRUE(checkNodes(variable->readBy, {}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {16}));
        }
        {
            auto varName = "__GetNeighbors_17";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_TRUE(variable->colNames.empty());
            EXPECT_TRUE(checkNodes(variable->readBy, {18}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {17}));
        }
        {
            auto varName = "__Project_18";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"__COL_0", "_vid"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {19}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {18}));
        }
        {
            auto varName = "__InnerJoin_19";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>({"id", "__COL_1", "__COL_0", "_vid"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {20}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {19}));
        }
        {
            auto varName = "__InnerJoin_20";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames,
                      std::vector<std::string>({"id", "__COL_1", "__COL_0", "_vid", "id"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {21}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {20}));
        }
        {
            auto varName = "__Project_21";
            auto* variable = symTable->getVar(varName);
            EXPECT_NE(variable, nullptr);
            EXPECT_EQ(variable->name, varName);
            EXPECT_EQ(variable->type, Value::Type::DATASET);
            EXPECT_EQ(variable->colNames, std::vector<std::string>({"like._dst"}));
            EXPECT_TRUE(checkNodes(variable->readBy, {}));
            EXPECT_TRUE(checkNodes(variable->writtenBy, {21}));
        }
    }
}
}  // namespace graph
}  // namespace nebula
