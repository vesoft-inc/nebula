/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "storage/query/GetDstBySrcProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

class GetDstBySrcTest : public ::testing::Test {
 public:
  void SetUp() override {
    fs::TempDir rootPath("/tmp/GetDstBySrcTest.XXXXXX");
    cluster_.reset(new mock::MockCluster());
    cluster_->initStorageKV(rootPath.path());
    env_ = cluster_->storageEnv_.get();
    totalParts_ = cluster_->getTotalParts();
    threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env_, totalParts_));
  }

  void TearDown() override {}

 protected:
  void verify(const std::vector<VertexID>& vertices,
              const std::vector<EdgeType>& edges,
              std::vector<VertexID>& expect) {
    auto req = buildRequest(vertices, edges);
    auto* processor = GetDstBySrcProcessor::instance(env_, nullptr, threadPool_.get());
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
    checkResponse(*resp.dsts_ref(), expect);
  }

 private:
  cpp2::GetDstBySrcRequest buildRequest(const std::vector<VertexID>& vertices,
                                        const std::vector<EdgeType>& edges) {
    std::hash<std::string> hash;
    cpp2::GetDstBySrcRequest req;
    req.space_id_ref() = 1;
    for (const auto& vertex : vertices) {
      PartitionID partId = (hash(vertex) % totalParts_) + 1;
      (*req.parts_ref())[partId].emplace_back(vertex);
    }
    for (const auto& edge : edges) {
      req.edge_types_ref()->emplace_back(edge);
    }
    return req;
  }

  void checkResponse(nebula::DataSet& actual, std::vector<VertexID>& dsts) {
    // sort to make sure the return order are same
    std::sort(actual.rows.begin(), actual.rows.end());
    nebula::DataSet expected({"_dst"});
    for (const auto& dst : dsts) {
      List list;
      list.emplace_back(dst);
      expected.emplace_back(std::move(list));
    }
    std::sort(expected.rows.begin(), expected.rows.end());
    EXPECT_EQ(actual, expected);
  }

 private:
  std::unique_ptr<mock::MockCluster> cluster_;
  StorageEnv* env_;
  int32_t totalParts_;
  std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
};

TEST_F(GetDstBySrcTest, BasicTest) {
  EdgeType serve = 101;
  EdgeType teammate = 102;

  {
    LOG(INFO) << "OneSrcOneOutEdgeType";
    std::vector<VertexID> vertices{"Tim Duncan"};
    std::vector<EdgeType> edges{serve};
    std::vector<VertexID> expect{"Spurs"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "OneSrcOneInEdgeType";
    std::vector<VertexID> vertices{"Rockets"};
    std::vector<EdgeType> edges{-serve};
    std::vector<VertexID> expect{"Tracy McGrady",
                                 "Russell Westbrook",
                                 "James Harden",
                                 "Chris Paul",
                                 "Carmelo Anthony",
                                 "Yao Ming",
                                 "Dwight Howard"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "OneSrcMultipleOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan"};
    std::vector<EdgeType> edges{serve, teammate};
    std::vector<VertexID> expect{"Spurs", "Tony Parker", "Manu Ginobili"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "OneSrcInOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan"};
    std::vector<EdgeType> edges{serve, -teammate};
    std::vector<VertexID> expect{"Spurs", "Tony Parker", "Manu Ginobili"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "OneSrcInOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan"};
    std::vector<EdgeType> edges{serve, teammate, -teammate};
    std::vector<VertexID> expect{"Spurs", "Tony Parker", "Manu Ginobili"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "MultipleSrcOneOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan", "Manu Ginobili"};
    std::vector<EdgeType> edges{serve};
    // The output has been deduped, so only one Spurs
    std::vector<VertexID> expect{"Spurs"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "MultipleSrcMultipleOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker", "Manu Ginobili", "Kobe Bryant"};
    std::vector<EdgeType> edges{serve, teammate};
    std::vector<VertexID> expect{"Spurs",
                                 "Lakers",
                                 "Hornets",
                                 "Tim Duncan",
                                 "Tony Parker",
                                 "Manu Ginobili",
                                 "Shaquille O'Neal"};
    verify(vertices, edges, expect);
  }
  {
    LOG(INFO) << "MultipleSrcInOutEdgeType";
    std::vector<VertexID> vertices = {"Tim Duncan", "Rockets"};
    std::vector<EdgeType> edges{serve, -serve, teammate, -teammate};
    std::vector<VertexID> expect{"Spurs",
                                 "Tony Parker",
                                 "Manu Ginobili",
                                 "Tracy McGrady",
                                 "Russell Westbrook",
                                 "James Harden",
                                 "Chris Paul",
                                 "Carmelo Anthony",
                                 "Yao Ming",
                                 "Dwight Howard"};
    verify(vertices, edges, expect);
  }
}

class GetDstBySrcConcurrentTest : public GetDstBySrcTest {
 public:
  void SetUp() override {
    FLAGS_query_concurrently = true;
    GetDstBySrcTest::SetUp();
  }

  void TearDown() override {
    FLAGS_query_concurrently = false;
  }
};

TEST_F(GetDstBySrcConcurrentTest, ConcurrentTest) {
  EdgeType serve = 101;
  {
    std::vector<VertexID> vertices = {"Spurs", "Rockets"};
    std::vector<EdgeType> edges{-serve};
    // clang-format off
    std::vector<VertexID> expect{
        "Aron Baynes", "Boris Diaw", "Cory Joseph", "Danny Green", "David West",
        "Dejounte Murray", "Jonathon Simmons", "Kyle Anderson", "LaMarcus Aldridge",
        "Manu Ginobili", "Marco Belinelli", "Pau Gasol", "Rudy Gay", "Tiago Splitter",
        "Tim Duncan", "Tony Parker", "Tracy McGrady", "Russell Westbrook", "James Harden",
        "Chris Paul", "Carmelo Anthony", "Yao Ming", "Dwight Howard"};
    // clang-format on
    verify(vertices, edges, expect);
  }
}

class GetDstBySrcTTLTest : public GetDstBySrcTest {
 public:
  void SetUp() override {
    FLAGS_mock_ttl_col = true;
    GetDstBySrcTest::SetUp();
  }

  void TearDown() override {
    FLAGS_mock_ttl_col = false;
  }
};

TEST_F(GetDstBySrcTTLTest, TTLTest) {
  EdgeType serve = 101;
  LOG(INFO) << "Read data when data valid";
  {
    std::vector<VertexID> vertices{"Tim Duncan"};
    std::vector<EdgeType> edges{serve};
    std::vector<VertexID> expect{"Spurs"};
    verify(vertices, edges, expect);
  }
  {
    std::vector<VertexID> vertices{"Rockets"};
    std::vector<EdgeType> edges{-serve};
    std::vector<VertexID> expect{"Tracy McGrady",
                                 "Russell Westbrook",
                                 "James Harden",
                                 "Chris Paul",
                                 "Carmelo Anthony",
                                 "Yao Ming",
                                 "Dwight Howard"};
    verify(vertices, edges, expect);
  }
  sleep(FLAGS_mock_ttl_duration + 1);
  LOG(INFO) << "Read data when data expired";
  {
    std::vector<VertexID> vertices{"Tim Duncan"};
    std::vector<EdgeType> edges{serve};
    std::vector<VertexID> expect{};
    verify(vertices, edges, expect);
  }
  {
    std::vector<VertexID> vertices{"Rockets"};
    std::vector<EdgeType> edges{-serve};
    std::vector<VertexID> expect{};
    verify(vertices, edges, expect);
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
