/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/webservice/Router.h"
#include "common/webservice/WebService.h"
#include "common/http/HttpClient.h"
#include <gtest/gtest.h>
#include "storage/http/StorageHttpDownloadHandler.h"
#include "storage/test/MockHdfsHelper.h"
#include "storage/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

DECLARE_string(meta_server_addrs);

namespace nebula {
namespace storage {

std::unique_ptr<hdfs::HdfsHelper> helper = std::make_unique<storage::MockHdfsOKHelper>();

class StorageHttpDownloadHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_ip = "127.0.0.1";
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpDownloadHandler.XXXXXX");
        cluster_ = std::make_unique<mock::MockCluster>();
        cluster_->initStorageKV(rootPath_->path());

        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(1);

        VLOG(1) << "Starting web service...";
        webSvc_ = std::make_unique<WebService>();
        auto& router = webSvc_->router();
        router.get("/download").handler([this](nebula::web::PathParams&&) {
            auto handler = new storage::StorageHttpDownloadHandler();
            std::vector<std::string> paths{rootPath_->path()};
            handler->init(helper.get(), pool_.get(), cluster_->storageEnv_->kvstore_, paths);
            return handler;
        });
        auto status = webSvc_->start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        cluster_.reset();
        rootPath_.reset();
        webSvc_.reset();
        pool_->stop();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<mock::MockCluster>  cluster_;
    std::unique_ptr<WebService> webSvc_;
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_;
};

TEST(StorageHttpDownloadHandlerTest, StorageDownloadTest) {
    {
        auto url = "/download";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_TRUE(resp.value().empty());
    }
    {
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=1&space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile download successfully", resp.value());
    }
    {
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=illegal-part&space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile download failed", resp.value());
    }
    {
        helper = std::make_unique<nebula::storage::MockHdfsExistHelper>();
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=1&space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile download failed", resp.value());
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpDownloadHandlerTestEnv());

    return RUN_ALL_TESTS();
}
