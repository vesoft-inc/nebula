/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>              // for stringPrintf
#include <folly/Try.h>                 // for Try, Try::throwUnle...
#include <folly/detail/Iterators.h>    // for operator!=, Iterato...
#include <folly/dynamic.h>             // for dynamic::item_iterator
#include <folly/dynamic.h>             // for dynamic
#include <folly/dynamic.h>             // for dynamic::item_iterator
#include <folly/dynamic.h>             // for dynamic
#include <folly/futures/Future.h>      // for Future
#include <folly/futures/Future.h>      // for Future::get, Future...
#include <folly/futures/Future.h>      // for Future
#include <folly/futures/Promise.h>     // for PromiseException::P...
#include <folly/init/Init.h>           // for init
#include <folly/json.h>                // for parseJson
#include <gflags/gflags_declare.h>     // for DECLARE_int32
#include <glog/logging.h>              // for INFO
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <stdint.h>                    // for int64_t, int32_t
#include <stdlib.h>                    // for system
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref, required...
#include <unistd.h>                    // for sleep

#include <atomic>         // for __atomic_base, atomic
#include <exception>      // for exception
#include <memory>         // for unique_ptr, allocator
#include <ostream>        // for operator<<, basic_o...
#include <string>         // for basic_string, opera...
#include <type_traits>    // for remove_reference<>:...
#include <unordered_map>  // for _Map_base<>::mapped...
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"              // for MetaClientOptions
#include "common/base/Logging.h"                  // for LOG, LogMessage
#include "common/base/StatusOr.h"                 // for StatusOr
#include "common/datatypes/HostAddr.h"            // for HostAddr
#include "common/datatypes/Value.h"               // for Value
#include "common/fs/FileUtils.h"                  // for FileUtils
#include "common/fs/TempDir.h"                    // for TempDir
#include "common/meta/SchemaManager.h"            // for SchemaManager
#include "common/network/NetworkUtils.h"          // for NetworkUtils
#include "common/process/ProcessUtils.h"          // for ProcessUtils
#include "common/thread/GenericWorker.h"          // for GenericWorker
#include "common/thrift/ThriftTypes.h"            // for TagID, ClusterID
#include "common/utils/IndexKeyUtils.h"           // for PropertyType
#include "interface/gen-cpp2/meta_types.h"        // for ColumnDef, ServiceC...
#include "interface/gen-cpp2/storage_types.h"     // for NewTag, AddVertices...
#include "mock/MockCluster.h"                     // for MockCluster
#include "mock/MockData.h"                        // for VertexData
#include "storage/CommonUtils.h"                  // for StorageEnv
#include "storage/mutate/AddVerticesProcessor.h"  // for AddVerticesProcessor
#include "storage/test/TestUtils.h"               // for checkAddVerticesData

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

class ElasticSearchBasicTest : public ::testing::Test {
 public:
  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/elasticsearch_bulk_test.XXXXXX");
    esPort_ = network::NetworkUtils::getAvailablePort();
    ASSERT_TRUE(getESEngine());
    startES();
    ASSERT_TRUE(waitESReady());
  }

  void TearDown() override {
    stopES();
    bgThread_->stop();
    bgThread_->wait();
  }

 protected:
  bool getESEngine() {
    auto esBin = "/opt/vesoft/toolset/es.tar.gz";
    auto unzip = "tar xzvf es.tar.gz -C ./es --strip-components 1";
    std::string cmd;
    if (fs::FileUtils::exist(esBin)) {
      auto cp = folly::stringPrintf("cp -f %s ./es.tar.gz", esBin);
      cmd = folly::stringPrintf(
          "cd %s && %s && mkdir es && %s", rootPath_->path(), cp.c_str(), unzip);
    } else {
      auto get =
          "wget https://artifacts.elastic.co/downloads/elasticsearch/"
          "elasticsearch-7.9.3-linux-x86_64.tar.gz -O es.tar.gz";
      cmd = folly::stringPrintf("cd %s && %s && mkdir es && %s", rootPath_->path(), get, unzip);
    }

    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
      LOG(ERROR) << "Command Failed: " << cmd;
      return false;
    }
    LOG(INFO) << "elasticsearch download complete.";
    return true;
  }

  void startESFun() {
    const char* cluster = &(rootPath_->path())[29];
    auto cmd = folly::stringPrintf(
        "cd %s/es && "
        "nohup ./bin/elasticsearch -E http.port=%u "
        "-E cluster.name=%s "
        "> ./output.log 2>&1 &",
        rootPath_->path(),
        esPort_,
        cluster);
    ASSERT_LE(0, system(cmd.c_str()));
    LOG(INFO) << "start elasticsearch : " << cmd;
  }
  void startES() {
    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&ElasticSearchBasicTest::startESFun, this);
  }

  bool waitESReady() {
    auto cmd =
        folly::stringPrintf("curl -XGET 'http://127.0.0.1:%u/_cluster/health?pretty'", esPort_);
    auto retry = 20;
    while (--retry > 0) {
      auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
      if (!ret.ok() || ret.value().empty()) {
        sleep(2);
        LOG(INFO) << "wait es start. retry : " << cmd;
        continue;
      }
      LOG(INFO) << "ret : " << ret.value();
      auto root = folly::parseJson(ret.value());
      auto result = root.find("cluster_name");
      if (result != root.items().end()) {
        LOG(INFO) << "elasticsearch started.";
        return true;
      } else {
        break;
      }
    }
    return false;
  }

  void stopES() {
    auto cmd = folly::stringPrintf(
        "ps -ef "
        "| grep \"Elasticsearch -E http.port=%u\" "
        "| awk '{print $2}' "
        "| xargs kill -9",
        esPort_);
    ASSERT_LE(0, system(cmd.c_str()));
  }

  std::vector<nebula::mock::VertexData> mockVertices(int64_t begin, int64_t end) {
    std::vector<nebula::mock::VertexData> ret;
    int64_t rowId = begin;
    while (++rowId <= end) {
      nebula::mock::VertexData data;
      data.vId_ = folly::stringPrintf("v_%ld", rowId);
      data.tId_ = tagId_;
      std::vector<Value> props;
      props.emplace_back(folly::stringPrintf("col1_%ld", rowId));
      props.emplace_back(folly::stringPrintf("col2_%ld", rowId));
      data.props_ = std::move(props);
      ret.emplace_back(std::move(data));
    }
    return ret;
  }

  cpp2::AddVerticesRequest mockVerticesReq(int32_t parts, int64_t begin, int64_t end) {
    nebula::storage::cpp2::AddVerticesRequest req;
    req.space_id_ref() = 1;
    req.if_not_exists_ref() = true;

    auto vertices = mockVertices(begin, end);

    for (auto& vertex : vertices) {
      nebula::storage::cpp2::NewVertex newVertex;
      nebula::storage::cpp2::NewTag newTag;
      auto partId = std::hash<std::string>()(vertex.vId_) % parts + 1;

      newTag.tag_id_ref() = tagId_;
      newTag.props_ref() = std::move(vertex.props_);

      std::vector<nebula::storage::cpp2::NewTag> newTags;
      newTags.push_back(std::move(newTag));

      newVertex.id_ref() = vertex.vId_;
      newVertex.tags_ref() = std::move(newTags);
      (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    return req;
  }

 protected:
  uint16_t esPort_;
  std::vector<HostAddr> peers_;
  std::unique_ptr<fs::TempDir> rootPath_;
  std::unique_ptr<thread::GenericWorker> bgThread_;
  GraphSpaceID spaceId_;
  TagID tagId_;
  // std::unique_ptr<storage::StorageEnv> env_{nullptr};
};

TEST_F(ElasticSearchBasicTest, SimpleTest) {
  FLAGS_heartbeat_interval_secs = 1;
  meta::cpp2::ServiceClient ftClient;
  ftClient.host_ref() = HostAddr("127.0.0.1", esPort_);
  const nebula::ClusterID kClusterId = 10;
  mock::MockCluster cluster;
  cluster.startMeta(folly::stringPrintf("%s/meta", rootPath_->path()));
  meta::MetaClientOptions options;
  HostAddr localHost(cluster.localIP(), network::NetworkUtils::getAvailablePort());
  options.localHost_ = localHost;
  options.clusterId_ = kClusterId;
  options.role_ = meta::cpp2::HostRole::STORAGE;
  cluster.initMetaClient(std::move(options));

  cluster.initStorageKV(rootPath_->path(), localHost, 1, true, true, {ftClient});
  auto* env = cluster.storageEnv_.get();
  {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto ret = env->schemaMan_->toGraphSpaceID("test_space");
    ASSERT_TRUE(ret.ok());
    spaceId_ = std::move(ret).value();
    auto* mClient = cluster.metaClient_.get();
    std::vector<meta::cpp2::ColumnDef> columns;
    columns.emplace_back();
    columns.back().name_ref() = "col1";
    columns.back().type.type_ref() = PropertyType::STRING;
    columns.emplace_back();
    columns.back().name_ref() = "col2";
    columns.back().type.type_ref() = PropertyType::STRING;
    meta::cpp2::Schema schema;
    schema.columns_ref() = std::move(columns);
    ret = mClient->createTagSchema(spaceId_, "tag1", schema).get();
    ASSERT_TRUE(ret.ok());
    tagId_ = std::move(ret).value();
  }
  {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mockVerticesReq(6, 0, 1000);
    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    LOG(INFO) << "Check data in kv store...";
    checkAddVerticesData(req, env, 1000, 0);
  }
  sleep(FLAGS_heartbeat_interval_secs * 10);
  {
    auto url =
        "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
        "-XGET "
        "\"http://127.0.0.1:%d/nebula_test_space_tag/_count?format=json\"";
    auto cmd = folly::stringPrintf(url, esPort_);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(ret.value().empty());
    auto root = folly::parseJson(ret.value());
    auto result = root.find("count");
    ASSERT_TRUE(result != root.items().end());
    ASSERT_TRUE(result->second.isInt());
    ASSERT_EQ(1000 * 2, result->second.getInt());
  }
  {
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mockVerticesReq(6, 1000, 2000);
    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
  sleep(FLAGS_heartbeat_interval_secs * 10);
  {
    auto url =
        "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
        "-XGET "
        "\"http://127.0.0.1:%d/nebula_test_space_tag/_count?format=json\"";
    auto cmd = folly::stringPrintf(url, esPort_);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(ret.value().empty());
    auto root = folly::parseJson(ret.value());
    auto result = root.find("count");
    ASSERT_TRUE(result != root.items().end());
    ASSERT_TRUE(result->second.isInt());
    ASSERT_EQ(1000 * 4, result->second.getInt());
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
