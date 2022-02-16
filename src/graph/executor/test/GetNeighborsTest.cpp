/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Conv.h>                // for to
#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Promise.h>     // for Promise::Prom...
#include <folly/futures/Promise.h>     // for PromiseExcept...
#include <folly/futures/Promise.h>     // for Promise::Prom...
#include <folly/futures/Promise.h>     // for PromiseExcept...
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>   // for make_unique
#include <string>   // for string, basic...
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Base.h"                           // for kVid
#include "common/datatypes/DataSet.h"                   // for Row, DataSet
#include "common/datatypes/Value.h"                     // for Value
#include "common/expression/PropertyExpression.h"       // for InputProperty...
#include "common/graph/Response.h"                      // for ExecutionResp...
#include "common/thrift/ThriftTypes.h"                  // for EdgeType
#include "graph/context/ExecutionContext.h"             // for ExecutionContext
#include "graph/context/QueryContext.h"                 // for QueryContext
#include "graph/context/Result.h"                       // for ResultBuilder
#include "graph/context/Symbols.h"                      // for SymbolTable
#include "graph/executor/query/GetNeighborsExecutor.h"  // for GetNeighborsE...
#include "graph/planner/plan/Query.h"                   // for GetNeighbors
#include "graph/service/RequestContext.h"               // for RequestContext
#include "graph/session/ClientSession.h"                // for SpaceInfo
#include "interface/gen-cpp2/meta_types.h"              // for Session, Spac...
#include "interface/gen-cpp2/storage_types.h"           // for EdgeProp, Expr

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
  auto reqDs = gnExe->buildRequestDataSet();

  DataSet expected;
  expected.colNames = {kVid};
  for (auto i = 0; i < 10; ++i) {
    Row row;
    row.values.emplace_back(folly::to<std::string>(i));
    expected.rows.emplace_back(std::move(row));
  }
  EXPECT_EQ(reqDs, expected);
}
}  // namespace graph
}  // namespace nebula
