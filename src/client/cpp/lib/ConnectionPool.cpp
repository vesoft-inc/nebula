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

bool ConnectionPool::init(const std::vector<ConnectionInfo> &addrInfo) {
    if (addrInfo.empty()) {
        LOG(ERROR) << "Input empty server addr";
        return false;
    }

    std::lock_guard<std::mutex> g(lock_);
    if (hasInit_) {
        LOG(WARNING) << "ConnectionPool has init, couldn’t init again";
        return false;
    }

    for (auto &item : addrInfo) {
        LOG(INFO) << "Service " << item.addr << ":" << item.port
                  << ", connNum: " << item.connectionNum << ", timeout: " << item.timeout;
        if (item.connectionNum < 0) {
            LOG(ERROR) << "Service: " << item.addr << ":" << item.port << " without connection";
            return false;
        }

        ConnectionMap connectionMap;
        std::vector<int32_t> unusedIds(item.connectionNum);
        std::string keyName = folly::stringPrintf("%s:%d",
                folly::trimWhitespace(item.addr).str().c_str(), item.port);

        for (auto i = 0u; i < item.connectionNum; ++i) {
            auto connection = std::make_unique<ConnectionThread>(
                    item.addr, item.port, item.timeout);
            auto ret = connection->createConnection();
            if (!ret) {
                LOG(ERROR) << "Connect to " << item.addr << ":" << item.port << " failed";
                return false;
            }

            unusedIds.emplace_back(i);
            connectionMap.emplace(i, std::move(connection));
        }
        unusedIds_.emplace(keyName, std::move(unusedIds));
        threads_.emplace(std::move(keyName), std::move(connectionMap));
    }

    hasInit_ = true;
    return true;
}

ConnectionThread* ConnectionPool::getConnection(const std::string &addr,
                                                uint32_t port,
                                                int32_t &indexId) {
    std::string keyName = folly::stringPrintf("%s:%d",
            folly::trimWhitespace(addr).str().c_str(), port);
    std::lock_guard<std::mutex> g(lock_);
    auto iter = unusedIds_.find(keyName);
    if (iter == unusedIds_.end()) {
        LOG(ERROR) << "Without " << keyName << " connection pool";
        return nullptr;
    }
    if (iter->second.empty()) {
        LOG(ERROR) << "No idle connections";
        return nullptr;
    }
    auto id = *iter->second.rbegin();
    indexId = id;
    iter->second.pop_back();
    return threads_[keyName][id].get();
}

void ConnectionPool::returnConnection(const std::string &addr,
                                      uint32_t port,
                                      int32_t indexId) {
    std::string keyName = folly::stringPrintf("%s:%d",
            folly::trimWhitespace(addr).str().c_str(), port);
    std::lock_guard<std::mutex> g(lock_);
    auto iter = unusedIds_.find(keyName);
    if (iter == unusedIds_.end()) {
        return;
    }
    if (std::find(iter->second.begin(), iter->second.end(), indexId) == iter->second.end()) {
        iter->second.emplace_back(indexId);
    }
}

}  // namespace graph
}  // namespace nebula


