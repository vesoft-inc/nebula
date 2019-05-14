/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/WebService.h"
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "webservice/NotFoundHandler.h"
#include "webservice/GetFlagsHandler.h"
#include "webservice/SetFlagsHandler.h"
#include "webservice/GetStatsHandler.h"

DEFINE_int32(ws_http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_int32(ws_h2_port, 11002, "Port to listen on with HTTP/2 protocol");
DEFINE_string(ws_ip, "127.0.0.1", "IP/Hostname to bind to");
DEFINE_int32(ws_threads, 4, "Number of threads for the web service.");

namespace nebula {
namespace {

class WebServiceHandlerFactory : public proxygen::RequestHandlerFactory {
public:
    explicit WebServiceHandlerFactory(HandlerGen&& genMap) : handlerGenMap_(genMap) {}

    void onServerStart(folly::EventBase*) noexcept override {
    }

    void onServerStop() noexcept override {
    }

    proxygen::RequestHandler* onRequest(proxygen::RequestHandler*,
                                        proxygen::HTTPMessage* msg) noexcept override {
        VLOG(2) << "Got request \"" << msg->getPath() << "\"";
        auto it = handlerGenMap_.find(msg->getPath());
        if (it != handlerGenMap_.end()) {
            return it->second();
        }

        // Unknown url path
        return new NotFoundHandler();
    }

private:
    HandlerGen handlerGenMap_;
};

}  // anonymous namespace


using proxygen::HTTPServer;
using proxygen::HTTPServerOptions;
using folly::SocketAddress;

std::unique_ptr<HTTPServer> WebService::server_;
std::unique_ptr<thread::NamedThread> WebService::wsThread_;
HandlerGen WebService::handlerGenMap_;


// static
void WebService::registerHandler(const std::string& path,
                                 std::function<proxygen::RequestHandler*()>&& gen) {
    handlerGenMap_.emplace(path, gen);
}


// static
Status WebService::start() {
    // Register the default handlers
    registerHandler("/get_flag", [] {
        return new GetFlagsHandler();
    });
    registerHandler("/get_flags", [] {
        return new GetFlagsHandler();
    });
    registerHandler("/set_flag", [] {
        return new SetFlagsHandler();
    });
    registerHandler("/set_flags", [] {
        return new SetFlagsHandler();
    });
    registerHandler("/get_stat", [] {
        return new GetStatsHandler();
    });
    registerHandler("/get_stats", [] {
        return new GetStatsHandler();
    });

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
    options.handlerFactories = proxygen::RequestHandlerChain()
        .addThen<WebServiceHandlerFactory>(std::move(handlerGenMap_))
        .build();
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
                        status = Status::Error(e.what());
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
    }
    return status;
}


// static
void WebService::stop() {
    server_->stop();
    wsThread_->join();
}

}  // namespace nebula

