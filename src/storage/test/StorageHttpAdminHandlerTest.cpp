/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"
#include "storage/http/StorageHttpAdminHandler.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

namespace nebula {
namespace storage {

using nebula::storage::TestUtils;
using nebula::fs::TempDir;

class StorageHttpAdminHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpAdminHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());
        schemaMan_ = TestUtils::mockSchemaMan();
        VLOG(1) << "Starting web service...";
        WebService::registerHandler("/admin", [this] {
            return new storage::StorageHttpAdminHandler(schemaMan_.get(), kv_.get());
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        schemaMan_.reset();
        kv_.reset();
        rootPath_.reset();
        VLOG(1) << "Web service stopped";
    }

    std::unique_ptr<kvstore::KVStore> kv_;
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<meta::SchemaManager> schemaMan_;
};


TEST(StoragehHttpAdminHandlerTest, AdminTest) {
    {
        auto url = "/admin";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Space should not be empty"));
    }
    {
        auto url = "/admin?space=xx";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Op should not be empty"));
    }
    {
        auto url = "/admin?space=xx&op=yy";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Can't find space xx"));
    }
    {
        auto url = "/admin?space=0&op=yy";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Unknown operation yy"));
    }
    {
        auto url = "/admin?space=0&op=flush";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("ok", resp.value());
    }
    {
        auto url = "/admin?space=0&op=compact";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("ok", resp.value());
    }
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

