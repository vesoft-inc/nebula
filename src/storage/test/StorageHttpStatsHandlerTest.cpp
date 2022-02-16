/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>           // for stringPrintf
#include <folly/init/Init.h>        // for init
#include <gflags/gflags_declare.h>  // for clstring
#include <glog/logging.h>           // for INFO
#include <gtest/gtest.h>            // for Message

#include <memory>   // for unique_ptr, make_u...
#include <ostream>  // for operator<<
#include <string>   // for string, basic_string

#include "common/base/Logging.h"   // for SetStderrLogging
#include "common/base/Status.h"    // for operator<<, Status
#include "common/base/StatusOr.h"  // for StatusOr
#include "common/fs/TempDir.h"
#include "common/http/HttpClient.h"                // for HttpClient
#include "kvstore/RocksEngineConfig.h"             // for FLAGS_enable_rocks...
#include "mock/MockCluster.h"                      // for MockCluster
#include "storage/http/StorageHttpStatsHandler.h"  // for StorageHttpStatsHa...
#include "webservice/Router.h"                     // for PathParams, Route
#include "webservice/WebService.h"                 // for WebService, FLAGS_...

namespace nebula {
namespace storage {

class StorageHttpStatsHandlerTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_ip = "127.0.0.1";
    FLAGS_ws_http_port = 0;
    FLAGS_ws_h2_port = 0;
    FLAGS_enable_rocksdb_statistics = true;
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpStatsHandler.XXXXXX");
    cluster_ = std::make_unique<mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());

    VLOG(1) << "Starting web service...";
    webSvc_ = std::make_unique<WebService>();
    auto& router = webSvc_->router();
    router.get("/rocksdb_stats").handler([](nebula::web::PathParams&&) {
      return new storage::StorageHttpStatsHandler();
    });
    auto status = webSvc_->start();
    ASSERT_TRUE(status.ok()) << status;
  }

  void TearDown() override {
    cluster_.reset();
    webSvc_.reset();
    rootPath_.reset();
    VLOG(1) << "Web service stopped";
  }

 protected:
  std::unique_ptr<mock::MockCluster> cluster_;
  std::unique_ptr<WebService> webSvc_;
  std::unique_ptr<fs::TempDir> rootPath_;
};

TEST(StorageHttpStatsHandlerTest, GetStatsTest) {
  {
    auto url = "/rocksdb_stats";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
  }
  {
    auto url = "/rocksdb_stats?stats=rocksdb.bytes.read";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    const std::string expect = "rocksdb.bytes.read=0\n";
    ASSERT_STREQ(expect.c_str(), resp.value().c_str());
  }
  // Get multiple stats
  {
    auto url = "/rocksdb_stats?stats=rocksdb.bytes.read,rocksdb.block.cache.add";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    const std::string expect = "rocksdb.block.cache.add=0\nrocksdb.bytes.read=0\n";
    ASSERT_STREQ(expect.c_str(), resp.value().c_str());
  }
  // Get multiple stats and return json
  {
    auto url =
        "/rocksdb_stats?stats=rocksdb.bytes.read,rocksdb.block.cache.add&"
        "format=json";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    const std::string expect =
        "[\n  {\n    \"rocksdb.block.cache.add\": 0\n  },"
        "\n  {\n    \"rocksdb.bytes.read\": 0\n  }\n]";
    ASSERT_STREQ(expect.c_str(), resp.value().c_str());
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpStatsHandlerTestEnv());

  return RUN_ALL_TESTS();
}
