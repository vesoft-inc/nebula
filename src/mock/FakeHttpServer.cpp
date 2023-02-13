/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "mock/FakeHttpServer.h"

#include "folly/synchronization/Baton.h"
#include "glog/logging.h"
#include "proxygen/httpserver/HTTPServer.h"
#include "proxygen/httpserver/HTTPServerOptions.h"
#include "proxygen/httpserver/ResponseBuilder.h"

namespace nebula {

void FakeHttpHandler::onRequest(std::unique_ptr<proxygen::HTTPMessage> message) noexcept {
  CHECK(message->getMethod());
  method_ = message->getMethod().value();
  auto headers = message->extractHeaders();
  headers.forEach(
      [this](std::string name, std::string value) { this->headers_.emplace_back(name, value); });
}

void FakeHttpHandler::onBody(std::unique_ptr<folly::IOBuf> buf) noexcept {
  if (body_) {
    body_->appendChain(std::move(buf));
  } else {
    body_ = std::move(buf);
  }
}

void FakeHttpHandler::onEOM() noexcept {
  std::tuple<int, std::map<std::string, std::string>, std::string> result;
  switch (method_) {
    case ::proxygen::HTTPMethod::PUT:
      result = onPut();
      break;
    case ::proxygen::HTTPMethod::GET:
      result = onGet();
      break;
    case ::proxygen::HTTPMethod::POST:
      result = onPost();
      break;
    case ::proxygen::HTTPMethod::DELETE:
      result = onDelete();
      break;
    default:
      CHECK(false);
      break;
  }
  auto builder = ::proxygen::ResponseBuilder(downstream_);
  builder.status(std::get<0>(result), "");
  for (auto& [name, value] : std::get<1>(result)) {
    builder.header(name, value);
  }
  builder.body(std::get<2>(result));
  builder.sendWithEOM();
}

void FakeHttpHandler::onUpgrade(proxygen::UpgradeProtocol) noexcept {
  // Do nothing
}

void FakeHttpHandler::requestComplete() noexcept {
  delete this;
}

void FakeHttpHandler::onError(proxygen::ProxygenError err) noexcept {
  LOG(FATAL) << ::proxygen::getErrorString(err);
}

FakeHttpServer::FakeHttpServer(int port) : port_(port) {}

void FakeHttpServer::start() {
  ::proxygen::HTTPServerOptions options;
  options.threads = 2;
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.enableContentCompression = false;
  options.handlerFactories =
      proxygen::RequestHandlerChain().addThen<FakeHttpHandlerFactory>().build();
  options.h2cEnabled = true;

  server_ = std::make_unique<::proxygen::HTTPServer>(std::move(options));
  std::vector<::proxygen::HTTPServer::IPConfig> ipconfig = {
      {folly::SocketAddress("127.0.0.1", port_, true), ::proxygen::HTTPServer::Protocol::HTTP}};

  server_->bind(ipconfig);
  folly::Baton baton;
  t_ = std::thread([this, &baton]() { this->server_->start([&baton]() { baton.post(); }); });
  baton.wait();
}

void FakeHttpServer::stop() {
  server_->stop();
}

void FakeHttpServer::join() {
  t_.join();
}

}  // namespace nebula
