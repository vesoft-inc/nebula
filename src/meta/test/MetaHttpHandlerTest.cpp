/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "webservice/WebService.h"
#include "process/ProcessUtils.h"
#include "meta/MetaHttpHandler.h"
#include "meta/test/TestUtils.h"
#include "fs/TempDir.h"
#include "fs/TempFile.h"

DECLARE_int32(load_data_interval_second);


namespace nebula {

using nebula::meta::TestUtils;
using nebula::fs::TempFile;
using nebula::fs::TempDir;

class MetaHttpHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";
        WebService::registerHandler("/status", [] {
            return new meta::MetaHttpHandler();
        });
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        VLOG(1) << "Web service stopped";
    }
};


bool getUrl(const std::string& urlPath, std::string& respBody) {
    auto url = folly::stringPrintf("http://%s:%d%s",
                                   FLAGS_ws_ip.c_str(),
                                   FLAGS_ws_http_port,
                                   urlPath.c_str());
    VLOG(1) << "Retrieving url: " << url;

    auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\" 2> /dev/null",
                                       url.c_str());
    auto result = ProcessUtils::runCommand(command.c_str());
    if (!result.ok()) {
        LOG(ERROR) << "Failed to run curl: " << result.status();
        return false;
    }
    respBody = result.value();
    return true;
}


TEST(MetaHttpHandlerTest, MetaStatusTest) {
    FLAGS_load_data_interval_second = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockServer(10001, rootPath.path(), FLAGS_meta_pid_file);

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
        ASSERT_TRUE(getUrl("/status123?daemon=status", resp));
        ASSERT_TRUE(resp.empty());
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::MetaHttpHandlerTestEnv());

    return RUN_ALL_TESTS();
}

