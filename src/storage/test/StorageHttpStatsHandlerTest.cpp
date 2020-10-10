/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "kvstore/RocksEngineConfig.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"
#include "storage/http/StorageHttpStatsHandler.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

namespace nebula {
namespace storage {

using nebula::storage::TestUtils;
using nebula::fs::TempDir;

class StorageHttpStatsHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        FLAGS_enable_rocksdb_statistics = true;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpStatsHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());

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
        webSvc_.reset();
        kv_.reset();
        rootPath_.reset();
        VLOG(1) << "Web service stopped";
    }

protected:
    std::unique_ptr<WebService> webSvc_;
    std::unique_ptr<kvstore::KVStore> kv_;
    std::unique_ptr<fs::TempDir> rootPath_;
};

TEST(StorageHttpStatsHandlerTest, GetStatsTest) {
    {
        auto url = "/rocksdb_stats";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
    }
    {
        auto url = "/rocksdb_stats?stats=rocksdb.bytes.read";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        const std::string expect = "rocksdb.bytes.read=0\n";
        ASSERT_STREQ(expect.c_str(), resp.value().c_str());
    }
    {
        auto url = "/rocksdb_stats?stats=rocksdb.bytes.read,rocksdb.block.cache.add&returnjson";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        const std::string expect = "[{\"value\":0,\"name\":\"rocksdb.block.cache.add\"},"
                "{\"value\":0,\"name\":\"rocksdb.bytes.read\"}]";
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
