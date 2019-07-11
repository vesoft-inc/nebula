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

namespace nebula {
namespace storage {

class StorageHttpIngestHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/MetaHttpIngestHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());

        WebService::registerHandler("/ingest", [this] {
            auto handler = new storage::StorageHttpIngestHandler();
            handler->init(kv_.get());
            return handler;
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
};

TEST(StorageHttpIngestHandlerTest, StorageIngestTest) {
    {
        auto url = "";
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile ingest successfully", resp);
    }
    {
        auto url = "";
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
