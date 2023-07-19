/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/context/QueryContext.h"
#include "graph/executor/query/GetNeighborsExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
class GetNeighborsTest : public testing::Test {
 protected:
  void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    {
      DataSet ds;
      ds.colNames = {"id", "col2"};
      for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i));
        row.values.emplace_back(i + 1);
        ds.rows.emplace_back(std::move(row));
      }
      ResultBuilder builder;
      builder.value(Value(std::move(ds)));
      qctx_->symTable()->newVariable("input_gn");
      qctx_->ectx()->setResult("input_gn", builder.build());
    }

    meta::cpp2::Session session;
    session.session_id_ref() = 0;
    session.user_name_ref() = "root";
    auto clientSession = ClientSession::create(std::move(session), nullptr);
    SpaceInfo spaceInfo;
    spaceInfo.name = "test_space";
    spaceInfo.id = 1;
    spaceInfo.spaceDesc.space_name_ref() = "test_space";
    clientSession->setSpace(std::move(spaceInfo));
    auto rctx = std::make_unique<RequestContext<ExecutionResponse>>();
    rctx->setSession(std::move(clientSession));
    qctx_->setRCtx(std::move(rctx));
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
};

TEST_F(GetNeighborsTest, BuildRequestDataSet) {
  auto* pool = qctx_->objPool();
  std::vector<EdgeType> edgeTypes;
  auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
  auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
  auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
  auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
  auto* vids = InputPropertyExpression::make(pool, "id");
  auto* gn = GetNeighbors::make(qctx_.get(),
                                nullptr,
                                0,
                                vids,
                                std::move(edgeTypes),
                                storage::cpp2::EdgeDirection::BOTH,
                                std::move(vertexProps),
                                std::move(edgeProps),
                                std::move(statProps),
                                std::move(exprs));
  gn->setInputVar("input_gn");

  auto gnExe = std::make_unique<GetNeighborsExecutor>(gn, qctx_.get());
  auto res = gnExe->buildRequestVids();
  auto reqVids = std::move(res).value();

  std::vector<Value> expectedVids;
  for (auto i = 0; i < 10; ++i) {
    expectedVids.emplace_back(folly::to<std::string>(i));
  }
  EXPECT_EQ(reqVids, expectedVids);
}
}  // namespace graph
}  // namespace nebula
