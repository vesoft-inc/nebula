/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "webservice/WebService.h"
#include "webservice/test/TestUtils.h"
#include "storage/StorageHttpDownloadHandler.h"
#include "storage/test/MockHdfsHelper.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace storage {

std::unique_ptr<hdfs::HdfsHelper> helper = std::make_unique<storage::MockHdfsOKHelper>();

class StorageHttpDownloadHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpDownloadHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path());

        pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
        pool_->start(1);
        auto poolPtr = pool_.get();

        VLOG(1) << "Starting web service...";
        WebService::registerHandler("/download", [poolPtr, this] {
            auto handler =  new storage::StorageHttpDownloadHandler();
            handler->init(helper.get(), poolPtr, kv_.get());
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
    std::unique_ptr<nebula::thread::GenericThreadPool> pool_;
};

TEST(StorageHttpDownloadHandlerTest, StorageDownloadTest) {
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/download", resp));
        ASSERT_TRUE(resp.empty());
    }
    {
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=1&space=0";
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile download successfully", resp);
    }
    {
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=illegal-part&space=0";
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile download failed", resp);
    }
    {
        helper = std::make_unique<nebula::storage::MockHdfsExistHelper>();
        auto url = "/download?host=127.0.0.1&port=9000&path=/data&parts=1&space=0";
        std::string resp;
        ASSERT_TRUE(getUrl(url, resp));
        ASSERT_EQ("SSTFile download failed", resp);
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

