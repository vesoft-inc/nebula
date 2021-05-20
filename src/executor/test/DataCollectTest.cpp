/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "executor/query/DataCollectExecutor.h"

namespace nebula {
namespace graph {
class DataCollectTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {kVid,
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            for (auto i = 0; i < 10; ++i) {
                Row row;
                // _vid
                row.values.emplace_back(folly::to<std::string>(i));
                // _stats = empty
                row.values.emplace_back(Value());
                // tag
                List tag;
                tag.values.emplace_back(0);
                tag.values.emplace_back(1);
                row.values.emplace_back(Value(tag));
                // edges
                List edges;
                for (auto j = 0; j < 2; ++j) {
                    List edge;
                    for (auto k = 0; k < 2; ++k) {
                        edge.values.emplace_back(k);
                    }
                    edge.values.emplace_back("2");
                    edge.values.emplace_back(j);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }

            DataSet ds2;
            ds2.colNames = {kVid,
                            "_stats",
                            "_tag:tag2:prop1:prop2",
                            "_edge:-edge2:prop1:prop2:_dst:_rank",
                            "_expr"};
            for (auto i = 10; i < 20; ++i) {
                Row row;
                // _vid
                row.values.emplace_back(folly::to<std::string>(i));
                // _stats = empty
                row.values.emplace_back(Value());
                // tag
                List tag;
                tag.values.emplace_back(0);
                tag.values.emplace_back(1);
                row.values.emplace_back(Value(tag));
                // edges
                List edges;
                for (auto j = 0; j < 2; ++j) {
                    List edge;
                    for (auto k = 0; k < 2; ++k) {
                        edge.values.emplace_back(k);
                    }
                    edge.values.emplace_back("2");
                    edge.values.emplace_back(j);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds2.rows.emplace_back(std::move(row));
            }

            List datasets;
            datasets.values.emplace_back(std::move(ds1));

            ResultBuilder builder;
            builder.value(Value(std::move(datasets))).iter(Iterator::Kind::kGetNeighbors);
            qctx_->symTable()->newVariable("input_datasets");
            qctx_->ectx()->setResult("input_datasets", builder.finish());
            qctx_->ectx()->setResult("input_datasets",
                                     ResultBuilder()
                                         .value(Value(std::move(ds2)))
                                         .iter(Iterator::Kind::kGetNeighbors)
                                         .finish());
        }
        {
            DataSet ds;
            ds.colNames = {"col1", "col2"};
            for (auto i = 0; i < 9; ++i) {
                Row row;
                row.values.emplace_back(i);
                row.values.emplace_back(i);
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->symTable()->newVariable("input_sequential");
            qctx_->ectx()->setResult("input_sequential",
                                     ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {kVid,
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            qctx_->symTable()->newVariable("empty_get_neighbors");
            qctx_->ectx()->setResult("empty_get_neighbors",
                                     ResultBuilder()
                                         .value(Value(ds))
                                         .iter(Iterator::Kind::kGetNeighbors)
                                         .finish());
            qctx_->ectx()->setResult("empty_get_neighbors",
                                     ResultBuilder()
                                         .value(Value(std::move(ds)))
                                         .iter(Iterator::Kind::kGetNeighbors)
                                         .finish());
        }
        {
            // for path with prop
            // <("0")-[:like]->("1")>
            DataSet vertices;
            vertices.colNames = {kVid, "player.name", "player.age"};
            for (auto i = 0; i < 2; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i));
                row.values.emplace_back(folly::to<std::string>(i));
                row.values.emplace_back(i + 20);
                vertices.rows.emplace_back(std::move(row));
            }
            qctx_->symTable()->newVariable("vertices");
            qctx_->ectx()->setResult("vertices",
                                     ResultBuilder()
                                         .value(Value(std::move(vertices)))
                                         .iter(Iterator::Kind::kProp)
                                         .finish());
            DataSet edges;
            edges.colNames = {
                "like._src", "like._dst", "like._rank", "like._type", "like.likeness"};
            Row edge;
            edge.values.emplace_back("0");
            edge.values.emplace_back("1");
            edge.values.emplace_back(0);
            edge.values.emplace_back(15);
            edge.values.emplace_back(90);
            edges.rows.emplace_back(std::move(edge));
            qctx_->symTable()->newVariable("edges");
            qctx_->ectx()->setResult("edges",
                                     ResultBuilder()
                                         .value(Value(std::move(edges)))
                                         .iter(Iterator::Kind::kProp)
                                         .finish());

            DataSet paths;
            paths.colNames = {"paths"};
            Vertex src("0", {});
            Vertex dst("1", {});
            Step step(std::move(dst), 15, "like", 0, {});
            Path path(std::move(src), {step});
            Row row;
            row.values.emplace_back(std::move(path));
            paths.rows.emplace_back(std::move(row));
            qctx_->symTable()->newVariable("paths");
            qctx_->ectx()->setResult("paths",
                                     ResultBuilder().value(Value(std::move(paths))).finish());
        }
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(DataCollectTest, CollectSubgraph) {
    auto* dc = DataCollect::make(qctx_.get(), DataCollect::DCKind::kSubgraph);
    dc->setInputVars({"input_datasets"});
    dc->setColNames(std::vector<std::string>{"_vertices", "_edges"});

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->outputVar());

    DataSet expected;
    expected.colNames = {"_vertices", "_edges"};
    auto& hist = qctx_->ectx()->getHistory("input_datasets");
    auto iter = hist[0].iter();
    auto* gNIter = static_cast<GetNeighborsIter*>(iter.get());
    Row row;
    std::unordered_set<Value> vids;
    std::unordered_set<std::tuple<Value, int64_t, int64_t, Value>> edgeKeys;
    List vertices;
    List edges;
    auto originVertices = gNIter->getVertices();
    for (auto& v : originVertices.values) {
        if (!v.isVertex()) {
            continue;
        }
        if (vids.emplace(v.getVertex().vid).second) {
            vertices.emplace_back(std::move(v));
        }
    }
    auto originEdges = gNIter->getEdges();
    for (auto& e : originEdges.values) {
        if (!e.isEdge()) {
            continue;
        }
        auto edgeKey = std::make_tuple(e.getEdge().src,
                                        e.getEdge().type,
                                        e.getEdge().ranking,
                                        e.getEdge().dst);
        if (edgeKeys.emplace(std::move(edgeKey)).second) {
            edges.emplace_back(std::move(e));
        }
    }
    row.values.emplace_back(std::move(vertices));
    row.values.emplace_back(std::move(edges));
    expected.rows.emplace_back(std::move(row));

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(DataCollectTest, RowBasedMove) {
    auto* dc = DataCollect::make(qctx_.get(), DataCollect::DCKind::kRowBasedMove);
    dc->setInputVars({"input_sequential"});
    dc->setColNames(std::vector<std::string>{"col1", "col2"});

    DataSet expected;
    auto& input = qctx_->ectx()->getResult("input_sequential");
    expected = input.value().getDataSet();

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->outputVar());

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(DataCollectTest, EmptyResult) {
    auto* dc = DataCollect::make(qctx_.get(), DataCollect::DCKind::kSubgraph);
    dc->setInputVars({"empty_get_neighbors"});
    dc->setColNames(std::vector<std::string>{"_vertices", "_edges"});

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->outputVar());

    DataSet expected;
    expected.colNames = {"_vertices", "_edges"};
    Row row;
    row.values.emplace_back(Value(List()));
    row.values.emplace_back(Value(List()));
    expected.rows.emplace_back(std::move(row));
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(DataCollectTest, PathWithProp) {
    auto* dc = DataCollect::make(qctx_.get(), DataCollect::DCKind::kPathProp);
    dc->setInputVars({"vertices", "edges", "paths"});
    dc->setColNames({"paths"});

    auto dcExe = std::make_unique<DataCollectExecutor>(dc, qctx_.get());
    auto future = dcExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dc->outputVar());

    DataSet expected;
    expected.colNames = {"paths"};
    Vertex src("0", {Tag("player", {{"age", Value(20)}, {"name", Value("0")}})});
    Vertex dst("1", {Tag("player", {{"age", Value(21)}, {"name", Value("1")}})});
    Step step(std::move(dst), 15, "like", 0, {{"likeness", 90}});
    Path path(std::move(src), {std::move(step)});
    Row row;
    row.values.emplace_back(std::move(path));
    expected.rows.emplace_back(std::move(row));
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}  // namespace graph
}  // namespace nebula
