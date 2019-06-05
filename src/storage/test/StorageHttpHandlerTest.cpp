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
#include "storage/StorageHttpHandler.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(load_data_interval_secs);

namespace nebula {

using nebula::storage::TestUtils;
using nebula::fs::TempDir;

class StorageHttpHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";
        WebService::registerHandler("/status", [] {
            return new storage::StorageHttpHandler();
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        VLOG(1) << "Web service stopped";
    }
};


TEST(StoragehHttpHandlerTest, StorageStatusTest) {
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/status", resp));
        ASSERT_EQ(std::string("status=running\n"), resp);
    }
    {
        std::string resp;
        ASSERT_TRUE(getUrl("", resp));
        ASSERT_EQ(std::string("status=running\n"), resp);
    }
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/status?daemon=status", resp));
        ASSERT_EQ(std::string("status=running\n"), resp);
    }
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/status?daemon=status&returnjson", resp));
        auto json = folly::parseJson(resp);
        ASSERT_TRUE(json.isArray());
        ASSERT_EQ(1UL, json.size());
        ASSERT_TRUE(json[0].isObject());
        ASSERT_EQ(2UL, json[0].size());

        auto it = json[0].find("name");
        ASSERT_NE(json[0].items().end(), it);
        ASSERT_TRUE(it->second.isString());
        ASSERT_EQ("status", it->second.getString());

        it = json[0].find("value");
        ASSERT_NE(json[0].items().end(), it);
        ASSERT_TRUE(it->second.isString());
        ASSERT_EQ("running", it->second.getString());
    }
    {
        std::string resp;
        ASSERT_TRUE(getUrl("/status123?deamon=status", resp));
        ASSERT_TRUE(resp.empty());
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::StorageHttpHandlerTestEnv());

    return RUN_ALL_TESTS();
}

