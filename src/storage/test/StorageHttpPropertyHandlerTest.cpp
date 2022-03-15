/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/json.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"
#include "mock/MockCluster.h"
#include "storage/http/StorageHttpPropertyHandler.h"
#include "storage/test/TestUtils.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"

namespace nebula {
namespace storage {

class StorageHttpStatsHandlerTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_ip = "127.0.0.1";
    FLAGS_ws_http_port = 0;
    FLAGS_enable_rocksdb_statistics = true;
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpPropertyHandler.XXXXXX");
    cluster_ = std::make_unique<mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());

    VLOG(1) << "Starting web service...";
    webSvc_ = std::make_unique<WebService>();
    auto& router = webSvc_->router();
    router.get("/rocksdb_property").handler([this](nebula::web::PathParams&&) {
      return new storage::StorageHttpPropertyHandler(cluster_->storageEnv_->schemaMan_,
                                                     cluster_->storageEnv_->kvstore_);
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

static std::string request(const std::string& url) {
  auto request =
      folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url.c_str());
  auto resp = http::HttpClient::get(request);
  EXPECT_TRUE(resp.ok());
  return resp.value();
}

static void checkInvalidRequest(const std::string& url, const std::string& errMsg) {
  ASSERT_EQ(0, request(url).find(errMsg));
}

TEST(StorageHttpPropertyHandlerTest, InvalidRequest) {
  checkInvalidRequest("/rocksdb_property", "Space should not be empty.");
  checkInvalidRequest("/rocksdb_property?space=xxx", "Space not found: xxx");
  checkInvalidRequest("/rocksdb_property?space=1", "Property should not be empty.");
  checkInvalidRequest("/rocksdb_property?space=1&property=yyy", "Property not found: yyy");
}

TEST(StorageHttpPropertyHandlerTest, ValidRequest) {
  {
    std::string expect =
        R"([
  {
    "Engine 0": "0",
    "Engine 1": "0"
  }
])";
    EXPECT_EQ(expect, request("/rocksdb_property?space=1&property=rocksdb.is-write-stopped"));
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
