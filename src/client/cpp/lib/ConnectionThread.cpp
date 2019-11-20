/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ConnectionThread.h"
#include <thrift/lib/cpp/async/TAsyncSocket.h>

namespace nebula {
namespace graph {

using apache::thrift::async::TAsyncSocket;
using apache::thrift::HeaderClientChannel;

ConnectionThread::~ConnectionThread() {
    if (!connection_) {
        return;
    }

    getEventBase()->runInEventBaseThreadAndWait([this] () { connection_.reset(); });
}

bool ConnectionThread::createConnection() {
    auto create = [&]() {
        auto socket = TAsyncSocket::newSocket(
                getEventBase(),
                addr_,
                port_,
                timeout_);
        connection_ = std::make_shared<cpp2::GraphServiceAsyncClient>(
                HeaderClientChannel::newChannel(socket));
    };
    getEventBase()->runInEventBaseThreadAndWait(create);
    return connection_ != nullptr;
}

folly::Future<StatusOr<cpp2::AuthResponse>>
ConnectionThread::authenticate(const std::string& username, const std::string& password) {
    using AuthPromise = folly::Promise<StatusOr<cpp2::AuthResponse>>;
    std::shared_ptr<AuthPromise> p = std::make_shared<AuthPromise>();
    auto future = p->getFuture();

    auto auth = [p, username, password, this] () mutable {
        auto handler = [p, username, password, this] (folly::Try<cpp2::AuthResponse> &&t) mutable {
            if (t.hasException()) {
                std::string error = folly::stringPrintf("RPC failure: %s",
                        t.exception().what().c_str());
                LOG(ERROR) << error;
                p->setValue(Status::Error(std::move(error)));
                return;
            }
            auto resp = std::move(t.value());
            sessionId_ = *(resp.get_session_id());
            LOG(INFO) << "sessionId = " << sessionId_;
            p->setValue(std::move(resp));
        };
        connection_->future_authenticate(username, password).then(getEventBase(), handler);
    };

    getEventBase()->runInEventBaseThread(auth);
    return future;
}

folly::Future<StatusOr<cpp2::ExecutionResponse>>
ConnectionThread::execute(const std::string &stmt) {
    using AuthPromise = folly::Promise<StatusOr<cpp2::ExecutionResponse>>;
    std::shared_ptr<AuthPromise> p = std::make_shared<AuthPromise>();
    auto future = p->getFuture();

    auto execute = [p, &stmt, this] () mutable {
        auto handler = [p, &stmt] (folly::Try<cpp2::ExecutionResponse> &&t) {
            if (t.hasException()) {
                std::string error = folly::stringPrintf("RPC failure: %s",
                        t.exception().what().c_str());
                LOG(ERROR) << error;
                p->setValue(Status::Error(std::move(error)));
                return;
            }

            p->setValue(std::move(t.value()));
        };
        connection_->future_execute(sessionId_, stmt).then(getEventBase(), handler);
    };
    getEventBase()->runInEventBaseThread(execute);
    return future;
}

folly::Future<Status> ConnectionThread::signout() {
    std::shared_ptr<folly::Promise<Status>> p = std::make_shared<folly::Promise<Status>>();
    auto future = p->getFuture();

    auto signout = [p, this] () mutable {
        auto handler = [p, this] (folly::Try<folly::Unit> &&t) {
            if (t.hasException()) {
                std::string error = folly::stringPrintf("RPC failure: %s",
                        t.exception().what().c_str());
                LOG(ERROR) << error;
                p->setValue(Status::Error(std::move(error)));
                return;
            }

            p->setValue(Status::OK());
            sessionId_ = 0;
        };
        connection_->future_signout(sessionId_).then(getEventBase(), handler);
    };
    getEventBase()->runInEventBaseThread(signout);
    return future;
}

}  // namespace graph
}  // namespace nebula
