/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/http/HttpClient.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "storage/test/TestUtils.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"

namespace nebula {
namespace storage {

class StorageHttpAdminHandlerTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_ip = "127.0.0.1";
    FLAGS_ws_http_port = 0;
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpAdminHandler.XXXXXX");
    cluster_ = std::make_unique<mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());

    VLOG(1) << "Starting web service...";
    webSvc_ = std::make_unique<WebService>();
    auto& router = webSvc_->router();
    router.get("/admin").handler([this](nebula::web::PathParams&&) {
      return new storage::StorageHttpAdminHandler(cluster_->storageEnv_->schemaMan_,
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
  std::unique_ptr<mock::MockCluster> cluster_{nullptr};
  std::unique_ptr<WebService> webSvc_{nullptr};
  std::unique_ptr<fs::TempDir> rootPath_{nullptr};
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

TEST(StorageHttpAdminHandlerTest, TestInvalidRequests) {
  checkInvalidRequest("/admin", "Space should not be empty");
  checkInvalidRequest("/admin?space=xx", "Op should not be empty");
  checkInvalidRequest("/admin?space=xx&op=yy", "Can't find space xx");
  checkInvalidRequest("/admin?space=1&op=yy", "Unknown operation yy");
}

TEST(StorageHttpAdminHandlerTest, TestSupportedOperations) {
  ASSERT_EQ("ok", request("/admin?space=1&op=flush"));
  ASSERT_EQ("ok", request("/admin?space=1&op=compact"));
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpAdminHandlerTestEnv());

  return RUN_ALL_TESTS();
}
