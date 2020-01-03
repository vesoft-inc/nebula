/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/json.h>
#include "webservice/WebService.h"
#include "fs/TempDir.h"
#include "http/HttpClient.h"

namespace nebula {

using nebula::fs::TempDir;

class StatusHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";
        auto status = WebService::start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        WebService::stop();
        VLOG(1) << "Web service stopped";
    }
};


TEST(StatusHandlerTest, SimpleTest) {
    std::string gitInfoSha = NEBULA_STRINGIFY(GIT_INFO_SHA);
    {
        auto url = "/status";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        auto json = folly::parseJson(resp.value());
        LOG(INFO) << folly::toPrettyJson(json);
        ASSERT_EQ("running", json["status"].asString());
        ASSERT_EQ(gitInfoSha, json["git_info_sha"].asString());
    }
    {
        auto url = "";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        auto json = folly::parseJson(resp.value());
        ASSERT_EQ("running", json["status"].asString());
        ASSERT_EQ(gitInfoSha, json["git_info_sha"].asString());
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    ::testing::AddGlobalTestEnvironment(new nebula::StatusHandlerTestEnv());
    return RUN_ALL_TESTS();
}

