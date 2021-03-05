/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/webservice/WebService.h"

#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/HTTPServerOptions.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "common/thread/NamedThread.h"
#include "common/webservice/NotFoundHandler.h"
#include "common/webservice/GetFlagsHandler.h"
#include "common/webservice/SetFlagsHandler.h"
#include "common/webservice/GetStatsHandler.h"
#include "common/webservice/Router.h"
#include "common/webservice/StatusHandler.h"

DEFINE_int32(ws_http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_int32(ws_h2_port, 11002, "Port to listen on with HTTP/2 protocol");
DEFINE_string(ws_ip, "0.0.0.0", "IP/Hostname to bind to");
DEFINE_int32(ws_threads, 4, "Number of threads for the web service.");

namespace nebula {
namespace {

class WebServiceHandlerFactory : public proxygen::RequestHandlerFactory {
public:
    explicit WebServiceHandlerFactory(const web::Router* router) : router_(router) {}

    void onServerStart(folly::EventBase*) noexcept override {
    }

    void onServerStop() noexcept override {
    }

    proxygen::RequestHandler* onRequest(proxygen::RequestHandler*,
                                        proxygen::HTTPMessage* msg) noexcept override {
        return router_->dispatch(msg);
    }

private:
    const web::Router* router_;
};

}  // anonymous namespace


using proxygen::HTTPServer;
using proxygen::HTTPServerOptions;
using folly::SocketAddress;

WebService::WebService(const std::string& name) {
    router_ = std::make_unique<web::Router>(name, this);
}

WebService::~WebService() {
    server_->stop();
    wsThread_->join();
}

Status WebService::start() {
    if (started_) {
        LOG(INFO) << "Web service has been started.";
        return Status::OK();
    }

    router().get("/flags").handler([](web::PathParams&& params) {
        DCHECK(params.empty());
        return new GetFlagsHandler();
    });
    router().put("/flags").handler([](web::PathParams&& params) {
        DCHECK(params.empty());
        return new SetFlagsHandler();
    });
    router().get("/stats").handler([](web::PathParams&& params) {
        DCHECK(params.empty());
        return new GetStatsHandler();
    });
    router().get("/status").handler([](web::PathParams&& params) {
        DCHECK(params.empty());
        return new StatusHandler();
    });
    router().get("/").handler([](web::PathParams&& params) {
        DCHECK(params.empty());
        return new StatusHandler();
    });

    started_ = true;

    std::vector<HTTPServer::IPConfig> ips = {
        {SocketAddress(FLAGS_ws_ip, FLAGS_ws_http_port, true), HTTPServer::Protocol::HTTP},
        {SocketAddress(FLAGS_ws_ip, FLAGS_ws_h2_port, true), HTTPServer::Protocol::HTTP2},
    };

    CHECK_GT(FLAGS_ws_threads, 0)
        << "The number of webservice threads must be greater than zero";

    HTTPServerOptions options;
    options.threads = static_cast<size_t>(FLAGS_ws_threads);
    options.idleTimeout = std::chrono::milliseconds(60000);
    options.enableContentCompression = false;
    options.handlerFactories =
        proxygen::RequestHandlerChain().addThen<WebServiceHandlerFactory>(router_.get()).build();
    options.h2cEnabled = true;

    server_ = std::make_unique<HTTPServer>(std::move(options));
    server_->bind(ips);

    std::condition_variable cv;
    std::mutex mut;
    auto status = Status::OK();
    bool serverStartedDone = false;

    // Start HTTPServer mainloop in a separate thread
    wsThread_ = std::make_unique<thread::NamedThread>(
        "webservice-listener",
        [&] () {
            server_->start(
                [&]() {
                    auto addresses = server_->addresses();
                    CHECK_EQ(addresses.size(), 2UL);
                    if (FLAGS_ws_http_port == 0) {
                        FLAGS_ws_http_port = addresses[0].address.getPort();
                    }
                    if (FLAGS_ws_h2_port == 0) {
                        FLAGS_ws_h2_port = addresses[1].address.getPort();
                    }
                    LOG(INFO) << "Web service started on "
                              << "HTTP[" << FLAGS_ws_http_port << "], "
                              << "HTTP2[" << FLAGS_ws_h2_port << "]";
                    {
                        std::lock_guard<std::mutex> g(mut);
                        serverStartedDone = true;
                    }
                    cv.notify_all();
                },
                [&] (std::exception_ptr eptr) {
                    CHECK(eptr);
                    try {
                        std::rethrow_exception(eptr);
                    } catch (const std::exception &e) {
                        status = Status::Error("%s", e.what());
                    }
                    {
                        std::lock_guard<std::mutex> g(mut);
                        serverStartedDone = true;
                    }
                    cv.notify_all();
                });
    });

    {
        std::unique_lock<std::mutex> lck(mut);
        cv.wait(lck, [&]() {
            return serverStartedDone;
        });

        if (!status.ok()) {
            LOG(ERROR) << "Failed to start web service: " << status;
            return status;
        }
    }
    return status;
}

}  // namespace nebula
