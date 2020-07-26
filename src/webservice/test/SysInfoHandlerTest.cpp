/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "fs/TempDir.h"
#include "http/HttpClient.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"
#include "storage/test/TestUtils.h"
#include "webservice/SysInfoHandler.h"
#include <gtest/gtest.h>

namespace nebula {
namespace storage {

class SysInfoHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/SysInfoHandler.XXXXXX");

        webSvc_ = std::make_unique<WebService>();
        auto& router = webSvc_->router();
        router.get("/sysinfo").handler([this](web::PathParams&&) {
            std::vector<std::string> dataPaths = {rootPath_->path()};
            auto handler = new SysInfoHandler();
            handler->init(dataPaths);
            return handler;
        });
        auto webStatus = webSvc_->start();
        ASSERT_TRUE(webStatus.ok()) << webStatus;
    }

    void TearDown() override {
        rootPath_.reset();
        webSvc_.reset();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<WebService> webSvc_;
};

TEST(SysInfoHandlerTest, SimpleSysInfoHandlerTest) {
    {
        auto url = "/sysinfo";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::storage::SysInfoHandlerTestEnv());

    return RUN_ALL_TESTS();
}
