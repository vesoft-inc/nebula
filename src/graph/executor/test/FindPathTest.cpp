// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include <gtest/gtest.h>

#include "graph/context/QueryContext.h"
#include "graph/executor/algo/BFSShortestPathExecutor.h"
#include "graph/executor/algo/MultiShortestPathExecutor.h"
#include "graph/executor/algo/ProduceAllPathsExecutor.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"

namespace nebula {
namespace graph {
class FindPathTest : public testing::Test {
 protected:
  Path createPath(const std::vector<std::string>& steps) {
    Path path;
    path.src = Vertex(steps[0], {});
    for (size_t i = 1; i < steps.size(); ++i) {
      path.steps.emplace_back(Step(Vertex(steps[i], {}), EDGE_TYPE, "like", EDGE_RANK, {}));
    }
    return path;
  }
  // Topology is below
  // a->b, a->c
  // b->a, b->c
  // c->a, c->f, c->g
  // d->a, d->c, d->e
  // e->b
  // f->h
  // g->f, g->h, g->k
  // h->x, k->x
  void singleSourceInit() {
    // From: {a}  To: {x}
    {  //  1 step
       //  From: a->b, a->c
      DataSet ds;
      ds.colNames = gnColNames_;
      Row row;
      row.values.emplace_back("a");
      row.values.emplace_back(Value());
      List edges;
      for (const auto& dst : {"b", "c"}) {
        List edge;
        edge.values.emplace_back(EDGE_TYPE);
        edge.values.emplace_back(dst);
        edge.values.emplace_back(EDGE_RANK);
        edges.values.emplace_back(std::move(edge));
      }
      row.values.emplace_back(edges);
      row.values.emplace_back(Value());
      ds.rows.emplace_back(std::move(row));
      single1StepFrom_ = std::move(ds);

      // To: x<-h, x<-k
      DataSet ds1;
      ds1.colNames = gnColNames_;
      Row row1;
      row1.values.emplace_back("x");
      row1.values.emplace_back(Value());
      List edges1;
      for (const auto& dst : {"h", "k"}) {
        List edge;
        edge.values.emplace_back(-EDGE_TYPE);
        edge.values.emplace_back(dst);
        edge.values.emplace_back(EDGE_RANK);
        edges1.values.emplace_back(std::move(edge));
      }
      row1.values.emplace_back(edges1);
      row1.values.emplace_back(Value());
      ds1.rows.emplace_back(std::move(row1));
      single1StepTo_ = std::move(ds1);
    }
    {  // 2 step
       // From: b->a, b->c, c->a, c->f, c->g
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"b", {"a", "c"}}, {"c", {"a", "f", "g"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      single2StepFrom_ = std::move(ds);

      // To : h<-f, h<-g, k<-g
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"h", {"f", "g"}}, {"k", {"g"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      single2StepTo_ = std::move(ds1);
    }
  }

  void mulitSourceInit() {
    // From {a, d} To {x, k}
    {  // 1 step
       // From: a->b, a->c, d->c, d->a, d->e
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"a", {"b", "c"}}, {"d", {"a", "c", "e"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      multi1StepFrom_ = std::move(ds);

      // To: x<-h, x<-k, k<-g
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"x", {"h", "k"}}, {"k", {"g"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      multi1StepTo_ = std::move(ds1);
    }
    {  // 2 step
      // From: b->a, b->c, c->a, c->f, c->g, e->b, a->b, a->c
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"b", {"a", "c"}}, {"c", {"a", "f", "g"}}, {"e", {"b"}}, {"a", {"b", "c"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      multi2StepFrom_ = std::move(ds);

      // To : h<-f, h<-g, k<-g, g<-c
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"h", {"f", "g"}}, {"k", {"g"}}, {"g", {"c"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      multi2StepTo_ = std::move(ds1);
    }
  }

  void allPathInit() {
    // From {a, d} To {x, k}
    {  // 1 step
       // From: a->b, a->c, d->c, d->a, d->e
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"a", {"b", "c"}}, {"d", {"a", "c", "e"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      all1StepFrom_ = std::move(ds);

      // To: x<-h, x<-k, k<-g
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"x", {"h", "k"}}, {"k", {"g"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      all1StepTo_ = std::move(ds1);
    }
    {  // 2 step
      // From: b->a, b->c, c->a, c->f, c->g, e->b, a->b, a->c
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data(
          {{"b", {"a", "c"}}, {"c", {"a", "f", "g"}}, {"e", {"b"}}, {"a", {"b", "c"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      all2StepFrom_ = std::move(ds);

      // To : h<-f, h<-g, k<-g, g<-c
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"h", {"f", "g"}}, {"k", {"g"}}, {"g", {"c"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      all2StepTo_ = std::move(ds1);
    }
    {  // 3 step
      // From: b->a, b->c, c->a, c->f, c->g, a->b, a->c, f->h, g->f, g->k, g->h
      DataSet ds;
      ds.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data({{"b", {"a", "c"}},
                                                                      {"c", {"a", "f", "g"}},
                                                                      {"f", {"h"}},
                                                                      {"a", {"b", "c"}},
                                                                      {"g", {"h", "f", "k"}}});
      for (const auto& src : data) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
      }
      all3StepFrom_ = std::move(ds);

      // To : f<-c, f<-g, g<-c, c<-a, c<-b, c<-d
      DataSet ds1;
      ds1.colNames = gnColNames_;
      std::unordered_map<std::string, std::vector<std::string>> data1(
          {{"c", {"a", "b", "d"}}, {"f", {"c", "g"}}, {"g", {"c"}}});
      for (const auto& src : data1) {
        Row row;
        row.values.emplace_back(src.first);
        row.values.emplace_back(Value());
        List edges;
        for (const auto& dst : src.second) {
          List edge;
          edge.values.emplace_back(-EDGE_TYPE);
          edge.values.emplace_back(dst);
          edge.values.emplace_back(EDGE_RANK);
          edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
      }
      all3StepTo_ = std::move(ds1);
    }
  }

  void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    singleSourceInit();
    mulitSourceInit();
    allPathInit();
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
  const int EDGE_TYPE = 1;
  const int EDGE_RANK = 0;
  DataSet single1StepFrom_;
  DataSet single1StepTo_;
  DataSet single2StepFrom_;
  DataSet single2StepTo_;
  DataSet multi1StepFrom_;
  DataSet multi1StepTo_;
  DataSet multi2StepFrom_;
  DataSet multi2StepTo_;
  DataSet all1StepFrom_;
  DataSet all1StepTo_;
  DataSet all2StepFrom_;
  DataSet all2StepTo_;
  DataSet all3StepFrom_;
  DataSet all3StepTo_;
  const std::vector<std::string> pathColNames_ = {"path"};
  const std::vector<std::string> gnColNames_ = {
      kVid, "_stats", "_edge:+like:_type:_dst:_rank", "_expr"};
};

TEST_F(FindPathTest, singleSourceShortestPath) {
  int steps = 5;
  std::string leftVidVar = "leftVid";
  std::string rightVidVar = "rightVid";
  std::string fromGNInput = "fromGNInput";
  std::string toGNInput = "toGNInput";
  qctx_->symTable()->newVariable(fromGNInput);
  qctx_->symTable()->newVariable(toGNInput);
  {
    qctx_->symTable()->newVariable(leftVidVar);
    DataSet fromVid;
    fromVid.colNames = {nebula::kVid};
    Row row;
    row.values.emplace_back("a");
    fromVid.rows.emplace_back(std::move(row));
    ResultBuilder builder;
    builder.value(std::move(fromVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(leftVidVar, builder.build());
  }
  {
    qctx_->symTable()->newVariable(rightVidVar);
    DataSet toVid;
    toVid.colNames = {nebula::kVid};
    Row row;
    row.values.emplace_back("x");
    toVid.rows.emplace_back(std::move(row));
    ResultBuilder builder;
    builder.value(std::move(toVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(rightVidVar, builder.build());
  }
  auto fromGN = StartNode::make(qctx_.get());
  auto toGN = StartNode::make(qctx_.get());

  auto* path = BFSShortestPath::make(qctx_.get(), fromGN, toGN, steps);
  path->setLeftVar(fromGNInput);
  path->setRightVar(toGNInput);
  path->setLeftVidVar(leftVidVar);
  path->setRightVidVar(rightVidVar);
  path->setColNames(pathColNames_);

  auto pathExe = std::make_unique<BFSShortestPathExecutor>(path, qctx_.get());
  // Step 1
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single1StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single1StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    auto resultDs = result.value().getDataSet();
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"b", "c"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"h", "k"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
  // 2 Step
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single2StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(single2StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    std::vector<std::vector<std::string>> paths(
        {{"a", "c", "f", "h", "x"}, {"a", "c", "g", "h", "x"}, {"a", "c", "g", "k", "x"}});
    for (const auto& p : paths) {
      Row row;
      row.values.emplace_back(createPath(p));
      expected.rows.emplace_back(std::move(row));
    }
    auto resultDs = result.value().getDataSet();
    std::sort(expected.rows.begin(), expected.rows.end());
    std::sort(resultDs.rows.begin(), resultDs.rows.end());
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
}

TEST_F(FindPathTest, multiSourceShortestPath) {
  int steps = 5;
  std::string leftVidVar = "leftVid";
  std::string rightVidVar = "rightVid";
  std::string fromGNInput = "fromGNInput";
  std::string toGNInput = "toGNInput";
  qctx_->symTable()->newVariable(fromGNInput);
  qctx_->symTable()->newVariable(toGNInput);
  {
    qctx_->symTable()->newVariable(leftVidVar);
    DataSet fromVid;
    fromVid.colNames = {nebula::kVid};
    Row row, row1;
    row.values.emplace_back("a");
    row1.values.emplace_back("d");
    fromVid.rows.emplace_back(std::move(row));
    fromVid.rows.emplace_back(std::move(row1));
    ResultBuilder builder;
    builder.value(std::move(fromVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(leftVidVar, builder.build());
  }
  {
    qctx_->symTable()->newVariable(rightVidVar);
    DataSet toVid;
    toVid.colNames = {nebula::kVid};
    Row row, row1;
    row.values.emplace_back("x");
    row1.values.emplace_back("k");
    toVid.rows.emplace_back(std::move(row));
    toVid.rows.emplace_back(std::move(row1));
    ResultBuilder builder;
    builder.value(std::move(toVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(rightVidVar, builder.build());
  }
  auto fromGN = StartNode::make(qctx_.get());
  auto toGN = StartNode::make(qctx_.get());

  auto* path = MultiShortestPath::make(qctx_.get(), fromGN, toGN, steps);
  path->setLeftVar(fromGNInput);
  path->setRightVar(toGNInput);
  path->setLeftVidVar(leftVidVar);
  path->setRightVidVar(rightVidVar);
  path->setColNames(pathColNames_);

  auto pathExe = std::make_unique<MultiShortestPathExecutor>(path, qctx_.get());
  // Step 1
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(multi1StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(multi1StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    auto resultDs = result.value().getDataSet();
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"b", "c", "a", "e"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"h", "k", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
  // 2 Step
  {
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(multi2StepFrom_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(fromGNInput, builder.build());
    }
    {
      ResultBuilder builder;
      List datasets;
      datasets.values.emplace_back(std::move(multi2StepTo_));
      builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
      qctx_->ectx()->setResult(toGNInput, builder.build());
    }
    auto future = pathExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(path->outputVar());

    DataSet expected;
    expected.colNames = pathColNames_;
    std::vector<std::vector<std::string>> paths({{"a", "c", "f", "h", "x"},
                                                 {"a", "c", "g", "h", "x"},
                                                 {"a", "c", "g", "k", "x"},
                                                 {"a", "c", "g", "k"},
                                                 {"d", "c", "f", "h", "x"},
                                                 {"d", "c", "g", "h", "x"},
                                                 {"d", "c", "g", "k", "x"},
                                                 {"d", "c", "g", "k"}});
    for (const auto& p : paths) {
      Row row;
      row.values.emplace_back(createPath(p));
      expected.rows.emplace_back(std::move(row));
    }
    auto resultDs = result.value().getDataSet();
    std::sort(expected.rows.begin(), expected.rows.end());
    std::sort(resultDs.rows.begin(), resultDs.rows.end());
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
    {
      DataSet expectLeftVid;
      expectLeftVid.colNames = {nebula::kVid};
      for (const auto& vid : {"b", "f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectLeftVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
      auto resultLeftVid = resultVid.value().getDataSet();
      std::sort(resultLeftVid.rows.begin(), resultLeftVid.rows.end());
      std::sort(expectLeftVid.rows.begin(), expectLeftVid.rows.end());
      EXPECT_EQ(resultLeftVid, expectLeftVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
      DataSet expectRightVid;
      expectRightVid.colNames = {nebula::kVid};
      for (const auto& vid : {"c", "f", "g"}) {
        Row row;
        row.values.emplace_back(vid);
        expectRightVid.rows.emplace_back(std::move(row));
      }
      auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
      auto resultRightVid = resultVid.value().getDataSet();
      std::sort(resultRightVid.rows.begin(), resultRightVid.rows.end());
      std::sort(expectRightVid.rows.begin(), expectRightVid.rows.end());
      EXPECT_EQ(resultRightVid, expectRightVid);
      EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
  }
}

TEST_F(FindPathTest, empthInput) {
  int steps = 5;
  std::string leftVidVar = "leftVid";
  std::string rightVidVar = "rightVid";
  std::string fromGNInput = "fromGNInput";
  std::string toGNInput = "toGNInput";
  qctx_->symTable()->newVariable(fromGNInput);
  qctx_->symTable()->newVariable(toGNInput);
  {
    qctx_->symTable()->newVariable(leftVidVar);
    DataSet fromVid;
    fromVid.colNames = {nebula::kVid};
    ResultBuilder builder;
    builder.value(std::move(fromVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(leftVidVar, builder.build());
  }
  {
    qctx_->symTable()->newVariable(rightVidVar);
    DataSet toVid;
    toVid.colNames = {nebula::kVid};
    ResultBuilder builder;
    builder.value(std::move(toVid)).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(rightVidVar, builder.build());
  }
  auto fromGN = StartNode::make(qctx_.get());
  auto toGN = StartNode::make(qctx_.get());

  auto* path = BFSShortestPath::make(qctx_.get(), fromGN, toGN, steps);
  path->setLeftVar(fromGNInput);
  path->setRightVar(toGNInput);
  path->setLeftVidVar(leftVidVar);
  path->setRightVidVar(rightVidVar);
  path->setColNames(pathColNames_);

  auto pathExe = std::make_unique<BFSShortestPathExecutor>(path, qctx_.get());
  {
    ResultBuilder builder;
    DataSet ds;
    ds.colNames = gnColNames_;
    builder.value(std::move(ds)).iter(Iterator::Kind::kGetNeighbors);
    qctx_->ectx()->setResult(fromGNInput, builder.build());
  }
  {
    ResultBuilder builder;
    DataSet ds;
    ds.colNames = gnColNames_;
    builder.value(std::move(ds)).iter(Iterator::Kind::kGetNeighbors);
    qctx_->ectx()->setResult(toGNInput, builder.build());
  }
  auto future = pathExe->execute();
  auto status = std::move(future).get();
  EXPECT_TRUE(status.ok());
  auto& result = qctx_->ectx()->getResult(path->outputVar());

  DataSet expected;
  expected.colNames = pathColNames_;
  auto resultDs = result.value().getDataSet();
  EXPECT_EQ(resultDs, expected);
  EXPECT_EQ(result.state(), Result::State::kSuccess);
  {
    DataSet expectLeftVid;
    expectLeftVid.colNames = {nebula::kVid};
    auto& resultVid = qctx_->ectx()->getResult(leftVidVar);
    auto resultLeftVid = resultVid.value().getDataSet();
    EXPECT_EQ(resultLeftVid, expectLeftVid);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
  }
  {
    DataSet expectRightVid;
    expectRightVid.colNames = {nebula::kVid};
    auto& resultVid = qctx_->ectx()->getResult(rightVidVar);
    auto resultRightVid = resultVid.value().getDataSet();
    EXPECT_EQ(resultRightVid, expectRightVid);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
  }
}

}  // namespace graph
}  // namespace nebula
