/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef MOCK_FAKEHTTPSERVER_H_
#define MOCK_FAKEHTTPSERVER_H_
#include <vector>

#include "proxygen/httpserver/HTTPServer.h"
#include "proxygen/httpserver/RequestHandler.h"
#include "proxygen/httpserver/RequestHandlerFactory.h"
namespace nebula {

class FakeHttpHandler : public proxygen::RequestHandler {
 public:
  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  std::vector<std::pair<std::string, std::string>> headers_;
  std::unique_ptr<folly::IOBuf> body_;
  ::proxygen::HTTPMethod method_;

  virtual std::tuple<int, std::map<std::string, std::string>, std::string> onPut() {
    return {200, {}, "PUT"};
  }
  virtual std::tuple<int, std::map<std::string, std::string>, std::string> onGet() {
    return {200, {}, "GET"};
  }
  virtual std::tuple<int, std::map<std::string, std::string>, std::string> onDelete() {
    return {200, {}, "DELETE"};
  }
  virtual std::tuple<int, std::map<std::string, std::string>, std::string> onPost() {
    return {200, {}, "POST"};
  }
};

class FakeHttpHandlerFactory : public ::proxygen::RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase*) noexcept override {}

  void onServerStop() noexcept override {}

  ::proxygen::RequestHandler* onRequest(::proxygen::RequestHandler*,
                                        ::proxygen::HTTPMessage*) noexcept override {
    return new FakeHttpHandler();
  }
};

class FakeHttpServer {
 public:
  explicit FakeHttpServer(int port);
  void start();
  void stop();
  void join();

 private:
  int port_;
  std::thread t_;
  std::unique_ptr<::proxygen::HTTPServer> server_ = nullptr;
};

}  // namespace nebula

#endif
