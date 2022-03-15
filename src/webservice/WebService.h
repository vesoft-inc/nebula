/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_WEBSERVICE_H_
#define WEBSERVICE_WEBSERVICE_H_

#include "common/base/Status.h"

DECLARE_int32(ws_http_port);
DECLARE_string(ws_ip);
DECLARE_int32(ws_threads);

#ifdef BUILD_STANDALONE
DECLARE_int32(ws_storage_http_port);
DECLARE_int32(ws_storage_threads);
#endif

namespace proxygen {
class HTTPServer;
class RequestHandler;
}  // namespace proxygen

namespace nebula {
namespace thread {
class NamedThread;
}  // namespace thread

namespace web {
class Router;
}  // namespace web

class WebService final {
 public:
  explicit WebService(const std::string& name = "");
  ~WebService();

  NG_MUST_USE_RESULT web::Router& router() {
    CHECK(!started_) << "Don't add routes after starting web server!";
    return *router_;
  }

  // To start the global web server.
  // Two ports would be bound, one for HTTP.
  // If FLAGS_ws_http_port is zero, an ephemeral port
  // would be assigned and set back to the gflag, respectively.
  NG_MUST_USE_RESULT Status start(uint16_t httpPort = FLAGS_ws_http_port);

  // Check whether web service is started
  bool started() const {
    return started_;
  }

 private:
  bool started_{false};
  std::unique_ptr<proxygen::HTTPServer> server_;
  std::unique_ptr<thread::NamedThread> wsThread_;
  std::unique_ptr<web::Router> router_;
};

}  // namespace nebula
#endif  // WEBSERVICE_WEBSERVICE_H_
