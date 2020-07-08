/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/GetNeighborsExecutor.h"

namespace nebula {
namespace graph {
class GetNeighborsTest : public testing::Test {
protected:
    static void SetUpTestCase() {
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
            qctx_->ectx()->setResult("input_gn", builder.finish());
        }
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> GetNeighborsTest::qctx_;

TEST_F(GetNeighborsTest, BuildRequestDataSet) {
    auto* plan = qctx_->plan();
    std::vector<EdgeType> edgeTypes;
    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto* vids = new InputPropertyExpression(new std::string("id"));
    auto* gn = GetNeighbors::make(
            plan,
            nullptr,
            0,
            plan->saveObject(vids),
            std::move(edgeTypes),
            storage::cpp2::EdgeDirection::BOTH,
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps),
            std::move(exprs));
    gn->setInputVar("input_gn");

    auto gnExe = std::make_unique<GetNeighborsExecutor>(gn, qctx_.get());
    auto status = gnExe->buildRequestDataSet();
    EXPECT_TRUE(status.ok());

    DataSet expected;
    expected.colNames = {kVid};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i));
        expected.rows.emplace_back(std::move(row));
    }
    auto& reqDs = gnExe->reqDs_;
    EXPECT_EQ(reqDs, expected);
}
}  // namespace graph
}  // namespace nebula
