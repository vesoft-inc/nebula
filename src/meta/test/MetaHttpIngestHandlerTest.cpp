/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/webservice/Router.h"
#include "common/webservice/WebService.h"
#include "common/fs/TempDir.h"
#include "common/thread/GenericThreadPool.h"
#include "common/http/HttpClient.h"
#include <gtest/gtest.h>
#include "meta/MetaHttpIngestHandler.h"
#include "meta/test/TestUtils.h"
#include "storage/http/StorageHttpIngestHandler.h"
#include <rocksdb/sst_file_writer.h>

DECLARE_int32(ws_storage_http_port);

namespace nebula {
namespace meta {

class MetaHttpIngestHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_ip = "127.0.0.1";
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/MetaHttpIngestHandler.XXXXXX");
        kv_ = MockCluster::initMetaKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 1);
        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(1);

        webSvc_ = std::make_unique<WebService>();

        auto& router = webSvc_->router();
        router.get("/ingest-dispatch").handler([this](nebula::web::PathParams&&) {
            auto handler = new meta::MetaHttpIngestHandler();
            handler->init(kv_.get(), pool_.get());
            return handler;
        });
        router.get("/ingest").handler([this](nebula::web::PathParams&&) {
            auto handler = new storage::StorageHttpIngestHandler();
            handler->init(kv_.get());
            return handler;
        });
        auto status = webSvc_->start();
        FLAGS_ws_storage_http_port = FLAGS_ws_http_port;
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        kv_.reset();
        rootPath_.reset();
        webSvc_.reset();
        pool_->stop();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<WebService> webSvc_;
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_;
};

TEST(MetaHttpIngestHandlerTest, MetaIngestTest) {
    auto path = "/tmp/MetaHttpIngestData.XXXXXX";
    std::unique_ptr<fs::TempDir> externalPath = std::make_unique<fs::TempDir>(path);
    auto partPath = folly::stringPrintf("%s/nebula/1/download/1", externalPath->path());
    ASSERT_TRUE(nebula::fs::FileUtils::makeDir(partPath));

    auto options = rocksdb::Options();
    auto env = rocksdb::EnvOptions();
    rocksdb::SstFileWriter writer{env, options};
    auto sstPath = folly::stringPrintf("%s/data.sst", partPath.c_str());
    auto status = writer.Open(sstPath);
    ASSERT_EQ(rocksdb::Status::OK(), status);

    for (auto i = 0; i < 10; i++) {
        status = writer.Put(folly::stringPrintf("key_%d", i),
                            folly::stringPrintf("val_%d", i));
        ASSERT_EQ(rocksdb::Status::OK(), status);
    }
    status = writer.Finish();
    ASSERT_EQ(rocksdb::Status::OK(), status);

    {
        auto url = "/ingest-dispatch";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_TRUE(resp.value().empty());
    }
    {
        auto url = "/ingest-dispatch?space=0";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::meta::MetaHttpIngestHandlerTestEnv());

    return RUN_ALL_TESTS();
}
