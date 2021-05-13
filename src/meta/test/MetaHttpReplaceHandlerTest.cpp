/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/process/ProcessUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/webservice/Router.h"
#include "common/webservice/WebService.h"
#include "common/fs/TempDir.h"
#include "common/thread/GenericThreadPool.h"
#include "meta/MetaHttpReplaceHostHandler.h"
#include "meta/test/TestUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/sst_file_writer.h>

DECLARE_int32(ws_storage_http_port);

namespace nebula {
namespace meta {


meta::MetaHttpReplaceHostHandler* gHandler = nullptr;
kvstore::KVStore*   gKVStore = nullptr;
std::vector<std::string> dumpKVStore(kvstore::KVStore* kvstore);

class MetaHttpReplaceHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        LOG(INFO) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/MetaHttpReplaceHandler.XXXXXX");
        kv_ = MockCluster::initMetaKV(rootPath_->path());
        TestUtils::createSomeHosts(kv_.get());
        TestUtils::assembleSpace(kv_.get(), 1, 1, 1);

        gKVStore = kv_.get();

        webSvc_ = std::make_unique<WebService>();
        auto &router = webSvc_->router();

        router.get("/replace").handler([&](nebula::web::PathParams&&) {
            gHandler = new meta::MetaHttpReplaceHostHandler();
            gHandler->init(gKVStore);
            return gHandler;
        });

        auto status = webSvc_->start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        kv_.reset();
        rootPath_.reset();
        webSvc_.reset();
        LOG(INFO) << "Web service stopped";
    }

private:
    std::unique_ptr<WebService> webSvc_;
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
};

StatusOr<std::string> silentCurl(const std::string& path) {
    auto command = folly::stringPrintf("/usr/bin/curl -Gs \"%s\"", path.c_str());
    return nebula::ProcessUtils::runCommand(command.c_str());
}

TEST(MetaHttpReplaceHandlerTest, FooTest) {
    std::vector<std::string> dump = dumpKVStore(gKVStore);
    for (auto& row : dump) {
        LOG(INFO) << "host=" << row;
    }

    std::string sFrom{"0.0.0.0"};
    std::string sTo{"66.66.66.66"};

    std::vector<std::string> beforeUpdate{sFrom};
    std::vector<std::string> afterUpdate{sTo};

    {
        // no [from]
        static const char *tmp = "http://127.0.0.1:%d/replace?to=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port, sTo.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_TRUE(result == beforeUpdate);
    }

    {
        // no [to]
        static const char *tmp = "http://127.0.0.1:%d/replace?&from=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port, sFrom.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_TRUE(result == beforeUpdate);
    }

    {
        // invalid [from]
        std::string invalidAddr = "10.10.10";
        const char *tmp = "http://127.0.0.1:%d/replace?from=%s&to=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port,
                                   invalidAddr.c_str(), sTo.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_TRUE(result == beforeUpdate);
    }

    {
        // invalid [to]
        std::string invalidAddr = "10.10.10";
        const char *tmp = "http://127.0.0.1:%d/replace?from=%s&to=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port,
                                       sFrom.c_str(), invalidAddr.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_TRUE(result == beforeUpdate);
    }

    {
        // valid [from] but not exist
        std::string notExistFrom = "1.1.1.1";
        const char *tmp = "http://127.0.0.1:%d/replace?from=%s&to=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port,
                                       notExistFrom.c_str(), sTo.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_EQ(result, beforeUpdate);
        LOG(INFO) << "valid [from] but not exist";
        for (auto& r : result) {
            LOG(INFO) << r;
        }
    }

    {
        // happy path
        static const char *tmp = "http://127.0.0.1:%d/replace?from=%s&to=%s";
        auto url = folly::stringPrintf(tmp, FLAGS_ws_http_port,
                                   sFrom.c_str(), sTo.c_str());
        silentCurl(url);
        auto result = dumpKVStore(gKVStore);
        EXPECT_EQ(result, afterUpdate);
    }
}

std::vector<std::string> dumpKVStore(kvstore::KVStore* kvstore) {
    LOG(INFO) << __func__ << " enter";
    std::vector<std::string> ret;
    // Get all spaces
    std::vector<GraphSpaceID> allSpaceId;
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    EXPECT_EQ(kvRet, nebula::cpp2::ErrorCode::SUCCEEDED);
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        allSpaceId.emplace_back(spaceId);
        iter->next();
    }

    LOG(INFO) << "allSpaceId.size()=" << allSpaceId.size();
    for (const auto& spaceId : allSpaceId) {
        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        kvRet = kvstore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        EXPECT_EQ(kvRet, nebula::cpp2::ErrorCode::SUCCEEDED);
        while (iter->valid()) {
            auto hostAddrs = MetaServiceUtils::parsePartVal(iter->val());
            LOG(INFO) << "hostAddrs.size()=" << hostAddrs.size();
            for (auto& hostAddr : hostAddrs) {
                ret.emplace_back(hostAddr.host);
            }
            iter->next();
        }
    }
    return ret;
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::meta::MetaHttpReplaceHandlerTestEnv());

    return RUN_ALL_TESTS();
}
