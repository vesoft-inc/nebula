/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>                         // for stringPrintf
#include <folly/init/Init.h>                      // for init
#include <gflags/gflags_declare.h>                // for clstring
#include <glog/logging.h>                         // for INFO
#include <gtest/gtest.h>                          // for Message
#include <gtest/gtest.h>                          // for TestPartResult
#include <proxygen/httpserver/RequestHandler.h>   // for ResponseBuilder
#include <proxygen/httpserver/ResponseBuilder.h>  // for ResponseBuilder
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for getErrorString, Pro...

#include <memory>  // for unique_ptr, allocator
#include <string>  // for basic_string, char_...

#include "common/base/Logging.h"     // for LogMessage, SetStde...
#include "common/base/Status.h"      // for operator<<, Status
#include "common/base/StatusOr.h"    // for StatusOr
#include "common/http/HttpClient.h"  // for HttpClient
#include "webservice/Common.h"       // for HttpStatusCode, Htt...
#include "webservice/Router.h"       // for Route, Router
#include "webservice/WebService.h"   // for WebService, FLAGS_w...

namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace nebula {
namespace http {

class HttpClientHandler : public proxygen::RequestHandler {
 public:
  HttpClientHandler() = default;

  void onRequest(std::unique_ptr<proxygen::HTTPMessage>) noexcept override {}

  void onBody(std::unique_ptr<folly::IOBuf>) noexcept override {}

  void onEOM() noexcept override {
    proxygen::ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body("HttpClientHandler successfully")
        .sendWithEOM();
  }

  void onUpgrade(proxygen::UpgradeProtocol) noexcept override {}

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(proxygen::ProxygenError error) noexcept override {
    LOG(ERROR) << "HttpClientHandler Error: " << proxygen::getErrorString(error);
  }
};
class HttpClientTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_ip = "127.0.0.1";
    FLAGS_ws_http_port = 0;
    LOG(INFO) << "Starting web service...";
    webSvc_ = std::make_unique<WebService>();

    auto& router = webSvc_->router();
    router.get("/path").handler([](auto&&) { return new HttpClientHandler(); });

    auto status = webSvc_->start();
    ASSERT_TRUE(status.ok()) << status;
  }

  void TearDown() override {
    webSvc_.reset();
    VLOG(1) << "Web service stopped";
  }

 private:
  std::unique_ptr<WebService> webSvc_;
};

TEST(HttpClient, get) {
  {
    auto url =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, "/path");
    auto result = HttpClient::get(url);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ("HttpClientHandler successfully", result.value());
  }
  {
    auto url = folly::stringPrintf(
        "http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, "/not_exist");
    auto result = HttpClient::get(url);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().empty());
  }
}

}  // namespace http
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  ::testing::AddGlobalTestEnvironment(new nebula::http::HttpClientTestEnv());
  return RUN_ALL_TESTS();
}
