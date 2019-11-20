/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ConnectionPool.h"

namespace nebula {
namespace graph {
// static
ConnectionPool& ConnectionPool::instance() {
    static ConnectionPool instance;
    return instance;
}

ConnectionPool::~ConnectionPool() {
    threads_.clear();
}

bool ConnectionPool::init(const std::string &addr,
                          uint16_t port,
                          int32_t connNum,
                          int32_t timeout) {
    std::lock_guard<std::mutex> g(lock_);
    if (hasInit_) {
        LOG(WARNING) << "ConnectionPool has init, couldn’t init again";
        return false;
    }
    if (connNum > 0) {
        threadNum_ = connNum;
    }
    unusedIds_.resize(threadNum_);

    for (int32_t i = 0; i < threadNum_; ++i) {
        auto connection = std::make_unique<ConnectionThread>(addr, port, timeout);
        auto ret = connection->createConnection();
        if (!ret) {
            LOG(ERROR) << "Connect to " << addr << ":" << port << " failed";
            return false;
        }

        unusedIds_.emplace_back(i);
        threads_.emplace(i, std::move(connection));
    }
    hasInit_ = true;
    return true;
}

ConnectionThread* ConnectionPool::getConnection(int32_t &connectionId) {
    std::lock_guard<std::mutex> g(lock_);
    if (unusedIds_.empty()) {
        LOG(ERROR) << "No idle connections";
        return nullptr;
    }
    auto id = *unusedIds_.crbegin();
    connectionId = id;
    unusedIds_.pop_back();
    return threads_[id].get();
}

void ConnectionPool::returnConnection(int32_t connectionId) {
    std::lock_guard<std::mutex> g(lock_);
    if (threads_.find(connectionId) == threads_.end()) {
        return;
    }
    unusedIds_.emplace_back(connectionId);
}

}  // namespace graph
}  // namespace nebula


