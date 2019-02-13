/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "webservice/WebService.h"
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "webservice/NotFoundHandler.h"
#include "webservice/GetFlagsHandler.h"
#include "webservice/SetFlagsHandler.h"

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
void WebService::start() {
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

    std::vector<HTTPServer::IPConfig> ips = {
        {SocketAddress(FLAGS_ws_ip, FLAGS_ws_http_port, true), HTTPServer::Protocol::HTTP},
        {SocketAddress(FLAGS_ws_ip, FLAGS_ws_h2_port, true), HTTPServer::Protocol::HTTP2},
    };

    CHECK_GT(FLAGS_ws_threads, 0)
        << "The number of webservice threads must be greater than zero";

    HTTPServerOptions options;
    options.threads = static_cast<size_t>(FLAGS_ws_threads);
    options.idleTimeout = std::chrono::milliseconds(60000);
    options.shutdownOn = {SIGINT, SIGTERM};
    options.enableContentCompression = false;
    options.handlerFactories = proxygen::RequestHandlerChain()
        .addThen<WebServiceHandlerFactory>(std::move(handlerGenMap_))
        .build();
    options.h2cEnabled = true;

    server_ = std::make_unique<HTTPServer>(std::move(options));
    server_->bind(ips);

    std::condition_variable cv;
    std::mutex mut;
    bool serverStarted = false;

    // Start HTTPServer mainloop in a separate thread
    wsThread_ = std::make_unique<thread::NamedThread>(
        "webservice-listener",
        [&] () {
            server_->start(
                [&]() {
                    LOG(INFO) << "Web service started";
                    {
                        std::lock_guard<std::mutex> g(mut);
                        serverStarted = true;
                    }
                    cv.notify_all();
                },
                [&] (std::exception_ptr) {
                    LOG(ERROR) << "Failed to start web service";
                });
        });

    {
        std::unique_lock<std::mutex> lck(mut);
        cv.wait(lck, [&]() {
            return serverStarted;
        });
    }
}


// static
void WebService::stop() {
    server_->stop();
    wsThread_->join();
}

}  // namespace nebula

