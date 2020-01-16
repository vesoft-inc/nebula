/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_CONNECTIONPOOL_H
#define CLIENT_CPP_LIB_CONNECTIONPOOL_H

#include "ConnectionThread.h"
#include "client/cpp/include/nebula/ExecutionResponse.h"

namespace nebula {
namespace graph {

class ConnectionPool final {
public:
    // static
    // Returns a singleton instance
    static ConnectionPool& instance();

    ~ConnectionPool();

    bool init(const std::vector<ConnectionInfo> &addrInfo);

    // If there has not idle socket, it will return nullptr
    // TODO: support to block when there has no idle socket
    ConnectionThread* getConnection(const std::string &addr, uint32_t port, int32_t &indexId);

    void returnConnection(const std::string &addr, uint32_t port, int32_t indexId);

    // TODO: add reconnect handle
private:
    ConnectionPool() = default;

private:
    // key: connection id, value: ConnectionThread
    using ConnectionMap = std::unordered_map<int32_t, std::unique_ptr<ConnectionThread>>;
    std::unordered_map<std::string, ConnectionMap>         threads_;
    std::unordered_map<std::string, std::vector<int32_t>>  unusedIds_;
    std::mutex                                             lock_;
    bool                                                   hasInit_{false};
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_CONNECTIONPOOL_H
