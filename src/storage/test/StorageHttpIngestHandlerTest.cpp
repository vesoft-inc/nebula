/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>           // for stringPrintf
#include <folly/init/Init.h>        // for init
#include <gflags/gflags_declare.h>  // for clstring
#include <glog/logging.h>           // for INFO
#include <gtest/gtest.h>            // for Message
#include <rocksdb/env.h>            // for EnvOptions
#include <rocksdb/options.h>        // for Options
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>  // for Status

#include <memory>  // for unique_ptr, make_...
#include <string>  // for basic_string

#include "common/base/Logging.h"                    // for SetStderrLogging
#include "common/base/Status.h"                     // for operator<<, Status
#include "common/base/StatusOr.h"                   // for StatusOr
#include "common/fs/FileUtils.h"                    // for FileUtils
#include "common/fs/TempDir.h"                      // for TempDir
#include "common/http/HttpClient.h"                 // for HttpClient
#include "mock/MockCluster.h"                       // for MockCluster
#include "storage/CommonUtils.h"                    // for StorageEnv
#include "storage/http/StorageHttpIngestHandler.h"  // for StorageHttpIngest...
#include "webservice/Router.h"                      // for PathParams, Route
#include "webservice/WebService.h"                  // for WebService, FLAGS...

namespace nebula {
namespace storage {

class StorageHttpIngestHandlerTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_ip = "127.0.0.1";
    FLAGS_ws_http_port = 0;
    VLOG(1) << "Starting web service...";

    rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpIngestHandler.XXXXXX");
    cluster_ = std::make_unique<mock::MockCluster>();
    cluster_->initStorageKV(rootPath_->path());

    auto partPath = folly::stringPrintf("%s/disk1/nebula/0/download/0", rootPath_->path());
    ASSERT_TRUE(nebula::fs::FileUtils::makeDir(partPath));

    auto options = rocksdb::Options();
    auto env = rocksdb::EnvOptions();
    rocksdb::SstFileWriter writer{env, options};

    auto sstPath = folly::stringPrintf("%s/data.sst", partPath.c_str());
    auto status = writer.Open(sstPath);
    ASSERT_EQ(rocksdb::Status::OK(), status);

    for (auto i = 0; i < 10; i++) {
      status = writer.Put(folly::stringPrintf("key_%d", i), folly::stringPrintf("val_%d", i));
      ASSERT_EQ(rocksdb::Status::OK(), status);
    }
    status = writer.Finish();
    ASSERT_EQ(rocksdb::Status::OK(), status);

    webSvc_ = std::make_unique<WebService>();
    auto& router = webSvc_->router();
    router.get("/ingest").handler([this](nebula::web::PathParams&&) {
      auto handler = new storage::StorageHttpIngestHandler();
      handler->init(cluster_->storageEnv_->kvstore_);
      return handler;
    });
    auto webStatus = webSvc_->start();
    ASSERT_TRUE(webStatus.ok()) << webStatus;
  }

  void TearDown() override {
    cluster_.reset();
    rootPath_.reset();
    webSvc_.reset();
    VLOG(1) << "Web service stopped";
  }

 private:
  std::unique_ptr<fs::TempDir> rootPath_;
  std::unique_ptr<mock::MockCluster> cluster_;
  std::unique_ptr<WebService> webSvc_;
};

TEST(StorageHttpIngestHandlerTest, StorageIngestTest) {
  {
    auto url = "/ingest?space=1";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    ASSERT_EQ("SSTFile ingest successfully", resp.value());
  }
  {
    auto url = "/ingest?space=0";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    ASSERT_EQ("SSTFile ingest failed", resp.value());
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpIngestHandlerTestEnv());

  return RUN_ALL_TESTS();
}
