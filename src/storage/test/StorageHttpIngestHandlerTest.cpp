/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "fs/TempDir.h"
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/StorageHttpIngestHandler.h"
#include <gtest/gtest.h>
#include <rocksdb/sst_file_writer.h>

namespace nebula {
namespace storage {

class StorageHttpIngestHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpIngestHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path(), 1);

        WebService::registerHandler("/ingest", [this] {
            auto handler = new storage::StorageHttpIngestHandler();
            handler->init(kv_.get());
            return handler;
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        kv_.reset();
        rootPath_.reset();
        WebService::stop();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
};

TEST(StorageHttpIngestHandlerTest, StorageIngestTest) {
    auto path = "/tmp/StorageHttpIngestData.XXXXXX";
    std::unique_ptr<fs::TempDir> externalPath_ = std::make_unique<fs::TempDir>(path);
    auto partPath = folly::stringPrintf("%s/1", externalPath_->path());
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
        auto url = "/ingest";
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("", resp);
    }
    {
        auto url = folly::stringPrintf("/ingest?path=%s&space=%d", externalPath_->path(), 0);
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile ingest successfully", resp);
    }
    {
        auto url = folly::stringPrintf("/ingest?path=%s&space=%d", "/tmp/maybe-not-exist", 0);
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile ingest failed", resp);
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
