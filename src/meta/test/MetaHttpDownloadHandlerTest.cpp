/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "http/HttpClient.h"
#include "webservice/WebService.h"
#include "meta/MetaHttpDownloadHandler.h"
#include "meta/test/MockHdfsHelper.h"
#include "meta/test/TestUtils.h"
#include "storage/StorageHttpDownloadHandler.h"
#include "fs/TempDir.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_string(pid_file);

namespace nebula {
namespace meta {

std::unique_ptr<hdfs::HdfsHelper> helper = std::make_unique<meta::MockHdfsOKHelper>();

class MetaHttpDownloadHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 12000;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/MetaHttpDownloadHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 2);

        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(3);

        WebService::registerHandler("/download-dispatch", [this] {
            auto handler = new meta::MetaHttpDownloadHandler();
            handler->init(kv_.get(), helper.get(), pool_.get());
            return handler;
        });
        WebService::registerHandler("/download", [this] {
            auto handler = new storage::StorageHttpDownloadHandler();
            std::vector<std::string> paths{rootPath_->path()};
            handler->init(helper.get(), pool_.get(), kv_.get(), paths);
            return handler;
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        kv_.reset();
        rootPath_.reset();
        WebService::stop();
        pool_->stop();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_;
};

TEST(MetaHttpDownloadHandlerTest, MetaDownloadTest) {
    {
        auto url = "/download-dispatch";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_TRUE(resp.value().empty());
    }
    {
        auto url = "/download-dispatch?host=127.0.0.1&port=9000&path=/data&space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile dispatch successfully", resp.value());
    }
    {
        helper = std::make_unique<nebula::meta::MockHdfsNotExistHelper>();
        auto url = "/download-dispatch?host=127.0.0.1&port=9000&path=/data&space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile dispatch failed", resp.value());
    }
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::meta::MetaHttpDownloadHandlerTestEnv());

    return RUN_ALL_TESTS();
}
