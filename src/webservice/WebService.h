/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WEBSERVICE_WEBSERVICE_H_
#define WEBSERVICE_WEBSERVICE_H_

#include "base/Status.h"

DECLARE_int32(ws_http_port);
DECLARE_int32(ws_h2_port);
DECLARE_string(ws_ip);
DECLARE_int32(ws_threads);

namespace proxygen {
class HTTPServer;
class RequestHandler;
}   // namespace proxygen

namespace nebula {
namespace thread {
class NamedThread;
}   // namespace thread

namespace web {
class Router;
}   // namespace web

class WebService final {
public:
    explicit WebService(const std::string& name = "");
    ~WebService();

    web::Router& router() {
        CHECK(!started_) << "Don't add routes after starting web server!";
        return *router_;
    }

    // To start the global web server.
    // Two ports would be bound, one for HTTP, another one for HTTP2.
    // If FLAGS_ws_http_port or FLAGS_ws_h2_port is zero, an ephemeral port
    // would be assigned and set back to the gflag, respectively.
    MUST_USE_RESULT Status start();

    // To stop the web service and join the internal threads
    void stop();

    // Check whether this web service is stopped
    bool stopped() const {
        return stopped_;
    }

private:
    bool started_{false};
    bool stopped_{false};
    std::unique_ptr<proxygen::HTTPServer> server_;
    std::unique_ptr<thread::NamedThread> wsThread_;
    std::unique_ptr<web::Router> router_;
};

}  // namespace nebula
#endif  // WEBSERVICE_WEBSERVICE_H_
