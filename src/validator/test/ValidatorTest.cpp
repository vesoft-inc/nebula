/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"
#include "planner/PlanNode.h"
#include "planner/Admin.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        // TODO: Need AdHocSchemaManager here.
    }

    void SetUp() override {
    }

    void TearDown() override {
    }

    std::unique_ptr<QueryContext> buildContext();

protected:
    ::testing::AssertionResult verifyPlan(const PlanNode* root,
                                          const std::vector<PlanNode::Kind>& expected) const {
        if (root == nullptr) {
            return ::testing::AssertionFailure() << "Get nullptr plan.";
        }

        std::vector<PlanNode::Kind> result;
        result.emplace_back(root->kind());
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        } else {
            return ::testing::AssertionFailure()
                        << "\n"
                        << "     Result: " << printPlan(result) << "\n"
                        << "     Expected: " << printPlan(expected);
        }
    }

    std::string printPlan(const std::vector<PlanNode::Kind>& plan) const {
        std::stringstream ss;
        ss << "[";
        for (auto& kind : plan) {
            ss << kind << ", ";
        }
        ss << "]";
        return ss.str();
    }

    void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result) const {
        switch (root->kind()) {
            case PlanNode::Kind::kUnknown:
                ASSERT_TRUE(false) << "Unkown Plan Node.";
            case PlanNode::Kind::kStart: {
                return;
            }
            case PlanNode::Kind::kGetNeighbors:
            case PlanNode::Kind::kGetVertices:
            case PlanNode::Kind::kGetEdges:
            case PlanNode::Kind::kReadIndex:
            case PlanNode::Kind::kFilter:
            case PlanNode::Kind::kProject:
            case PlanNode::Kind::kSort:
            case PlanNode::Kind::kLimit:
            case PlanNode::Kind::kAggregate:
            case PlanNode::Kind::kSwitchSpace:
            case PlanNode::Kind::kDedup: {
                auto* current = static_cast<const SingleInputNode*>(root);
                result.emplace_back(current->input()->kind());
                bfsTraverse(current->input(), result);
                break;
            }
            case PlanNode::Kind::kCreateSpace:
            case PlanNode::Kind::kCreateTag:
            case PlanNode::Kind::kCreateEdge:
            case PlanNode::Kind::kDescSpace:
            case PlanNode::Kind::kDescTag:
            case PlanNode::Kind::kDescEdge:
            case PlanNode::Kind::kInsertVertices:
            case PlanNode::Kind::kInsertEdges: {
                // TODO: DDLs and DMLs are kind of single input node.
            }
            case PlanNode::Kind::kUnion:
            case PlanNode::Kind::kIntersect:
            case PlanNode::Kind::kMinus: {
                auto* current = static_cast<const BiInputNode*>(root);
                result.emplace_back(current->left()->kind());
                result.emplace_back(current->right()->kind());
                bfsTraverse(current->left(), result);
                bfsTraverse(current->right(), result);
                break;
            }
            case PlanNode::Kind::kSelector: {
                auto* current = static_cast<const Selector*>(root);
                result.emplace_back(current->input()->kind());
                result.emplace_back(current->then()->kind());
                if (current->otherwise() != nullptr) {
                    result.emplace_back(current->otherwise()->kind());
                }
                bfsTraverse(current->input(), result);
                bfsTraverse(current->then(), result);
                if (current->otherwise() != nullptr) {
                    bfsTraverse(current->otherwise(), result);
                }
                break;
            }
            case PlanNode::Kind::kLoop: {
                auto* current = static_cast<const Loop*>(root);
                result.emplace_back(current->input()->kind());
                result.emplace_back(current->body()->kind());
                bfsTraverse(current->input(), result);
                bfsTraverse(current->body(), result);
                break;
            }
            default:
                LOG(FATAL) << "Unkown PlanNode: " << static_cast<int64_t>(root->kind());
        }
    }

protected:
    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
};

std::shared_ptr<ClientSession> ValidatorTest::session_;
meta::SchemaManager* ValidatorTest::schemaMng_;

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_);
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
}


TEST_F(ValidatorTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        auto plan = context->plan();
        ASSERT_NE(plan, nullptr);
        using PK = nebula::graph::PlanNode::Kind;
        std::vector<PlanNode::Kind> expected = {
            PK::kLoop, PK::kStart, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}
}  // namespace graph
}  // namespace nebula
