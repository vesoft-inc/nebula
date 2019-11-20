/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_CONNECTIONTHREAD_H
#define CLIENT_CPP_LIB_CONNECTIONTHREAD_H

#include "base/Base.h"
#include "base/StatusOr.h"
#include <folly/io/async/ScopedEventBaseThread.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include "gen-cpp2/GraphServiceAsyncClient.h"

namespace nebula {
namespace graph {

class ConnectionThread : public folly::ScopedEventBaseThread {
public:
    ConnectionThread(const std::string &addr, /* the add of graphd */
                     uint16_t port,           /* the port of graphd */
                     int32_t timeout = 1000   /* ms */)
        : addr_(addr),
        port_(port),
        timeout_(timeout) {}

    ~ConnectionThread();

    bool createConnection();

    folly::Future<StatusOr<cpp2::AuthResponse>> authenticate(const std::string& username,
                                                             const std::string& password);

    folly::Future<StatusOr<cpp2::ExecutionResponse>> execute(const std::string &stmt);

    folly::Future<Status> signout();

private:
    std::shared_ptr<cpp2::GraphServiceAsyncClient>    connection_;
    std::string                                       addr_;
    uint16_t                                          port_;
    int32_t                                           timeout_;
    int64_t                                           sessionId_{0};
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_CONNECTIONTHREAD_H
