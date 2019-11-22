/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_CONNECTIONPOOL_H
#define CLIENT_CPP_LIB__CONNECTIONPOOL_H

#include "ConnectionThread.h"

namespace nebula {
namespace graph {

class ConnectionPool final {
public:
    // static
    // Returns a singleton instance
    static ConnectionPool& instance();

    ~ConnectionPool();

    bool init(const std::string &addr,   /* the addr of graphd */
              uint16_t port,             /* the port of graphd */
              uint16_t connNum = 10,     /* the number of connections */
              int32_t timeout = 1000     /* ms */);

    // If there has not idle socket, it will return nullptr
    // TODO: support to block when there has no idle socket
    ConnectionThread* getConnection(int32_t &indexId);

    void returnConnection(int32_t indexId);

private:
    ConnectionPool() = default;

private:
    // key: connection id, value: ConnectionThread
    using ConnectionMap = std::unordered_map<int32_t, std::unique_ptr<ConnectionThread>>;
    ConnectionMap                                      threads_;
    std::vector<int32_t>                               unusedIds_;
    std::mutex                                         lock_;
    std::atomic_int                                    nextThreadToUse_;
    int32_t                                            threadNum_{10};
    bool                                               hasInit_{false};
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_CONNECTIONPOOL_H
