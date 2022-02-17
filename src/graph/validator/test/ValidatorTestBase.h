/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_
#define _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_

#include <folly/String.h>           // for join
#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException...
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>       // for allocator, uniqu...
#include <string>       // for string, basic_st...
#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector, operator==

#include "common/base/Base.h"
#include "common/base/Logging.h"         // for CheckNotNull
#include "common/base/ObjectPool.h"      // for ObjectPool
#include "common/base/Status.h"          // for Status, NG_RETUR...
#include "common/base/StatusOr.h"        // for StatusOr
#include "common/charset/Charset.h"      // for CharsetInfo
#include "common/graph/Response.h"       // for ExecutionResponse
#include "graph/context/QueryContext.h"  // for QueryContext
#include "graph/context/ValidateContext.h"
#include "graph/planner/PlannersRegister.h"    // for PlannersRegister
#include "graph/planner/plan/ExecutionPlan.h"  // for ExecutionPlan
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"  // for PlanNode::Kind
#include "graph/planner/plan/Query.h"
#include "graph/service/RequestContext.h"            // for RequestContext
#include "graph/session/ClientSession.h"             // for SpaceInfo, Clien...
#include "graph/util/AstUtils.h"                     // for AstUtils
#include "graph/validator/Validator.h"               // for Validator
#include "graph/validator/test/MockIndexManager.h"   // for MockIndexManager
#include "graph/validator/test/MockSchemaManager.h"  // for MockSchemaManager
#include "interface/gen-cpp2/meta_types.h"           // for Session, SpaceDesc
#include "parser/GQLParser.h"                        // for GQLParser
#include "parser/Sentence.h"                         // for Sentence

namespace nebula {
namespace graph {

class ValidatorTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    meta::cpp2::Session session;
    session.session_id_ref() = 0;
    session.user_name_ref() = "root";
    session_ = ClientSession::create(std::move(session), nullptr);
    SpaceInfo spaceInfo;
    spaceInfo.name = "test_space";
    spaceInfo.id = 1;
    spaceInfo.spaceDesc.space_name_ref() = "test_space";
    session_->setSpace(std::move(spaceInfo));
    schemaMng_ = CHECK_NOTNULL(MockSchemaManager::makeUnique());
    indexMng_ = CHECK_NOTNULL(MockIndexManager::makeUnique());
    pool_ = std::make_unique<ObjectPool>();
    PlannersRegister::registerPlanners();
  }

  StatusOr<QueryContext*> validate(const std::string& query) {
    VLOG(1) << "query: " << query;
    auto qctx = buildContext();
    auto result = GQLParser(qctx).parse(query);
    if (!result.ok()) {
      return std::move(result).status();
    }
    NG_RETURN_IF_ERROR(AstUtils::reprAstCheck(*result.value(), qctx));

    auto sentences = pool_->add(std::move(result).value().release());
    NG_RETURN_IF_ERROR(Validator::validate(sentences, qctx));
    return qctx;
  }

  QueryContext* buildContext() {
    auto rctx = std::make_unique<RequestContext<ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = pool_->add(new QueryContext());
    qctx->setRCtx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_.get());
    qctx->setIndexManager(indexMng_.get());
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
  }

  static Status EqSelf(const PlanNode* l, const PlanNode* r);

  static Status Eq(const PlanNode* l, const PlanNode* r);

  ::testing::AssertionResult checkResult(const std::string& query,
                                         const std::vector<PlanNode::Kind>& expected = {},
                                         const std::vector<std::string>& rootColumns = {}) {
    auto status = validate(query);
    if (!status) {
      return ::testing::AssertionFailure() << std::move(status).status().toString();
    }
    auto qctx = std::move(status).value();
    if (qctx == nullptr) {
      return ::testing::AssertionFailure() << "qctx is nullptr";
    }
    if (expected.empty()) {
      return ::testing::AssertionSuccess();
    }
    auto plan = qctx->plan();
    auto assertResult = verifyPlan(plan->root(), expected);
    if (!assertResult) {
      return assertResult;
    }
    if (rootColumns.empty()) {
      return ::testing::AssertionSuccess();
    }
    auto outputColumns = plan->root()->colNames();
    if (outputColumns == rootColumns) {
      return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << "Columns of root plan node are different: " << folly::join(",", outputColumns)
           << " vs. " << folly::join(",", rootColumns);
  }

  static ::testing::AssertionResult verifyPlan(const PlanNode* root,
                                               const std::vector<PlanNode::Kind>& expected) {
    if (root == nullptr) {
      return ::testing::AssertionFailure() << "Get nullptr plan.";
    }

    std::vector<PlanNode::Kind> result;
    bfsTraverse(root, result);
    if (result == expected) {
      return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << "\n\tResult: " << result << "\n\tExpected: " << expected;
  }

  static void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result);

 protected:
  std::shared_ptr<ClientSession> session_;
  std::unique_ptr<MockSchemaManager> schemaMng_;
  std::unique_ptr<MockIndexManager> indexMng_{nullptr};
  std::unique_ptr<Sentence> sentences_;
  std::unique_ptr<ObjectPool> pool_;
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan);

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_TEST_VALIDATORTESTBASE_H_
