/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METACLIENT_H_
#define META_METACLIENT_H_

#include "base/Base.h"
#include "gen-cpp2/MetaServiceAsyncClient.h"
#include "base/Status.h"
#include "base/StatusOr.h"
#include <folly/executors/IOThreadPoolExecutor.h>

namespace nebula {
namespace meta {

class MetaClient {
public:
    MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
               std::vector<HostAddr> addrs)
        : ioThreadPool_(ioThreadPool)
        , addrs_(std::move(addrs)) {
        updateActiveHost();
     }

    virtual ~MetaClient() = default;

    Status createNode(std::string path, std::string val);

    Status setNode(std::string path, std::string val);

    StatusOr<std::string> getNode(std::string path);

    StatusOr<std::vector<std::string>> listChildren(std::string path);

    Status removeNode(std::string path);

private:
    void updateActiveHost() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, addrs_.size() - 1);
        active_ = addrs_[dis(gen)];
    }

    template<typename RESP>
    Status handleResponse(const RESP& resp);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(std::shared_ptr<meta::cpp2::MetaServiceAsyncClient>, Request)
                >::type::value_type
    >
    Response collectResponse(Request req, RemoteFunc remoteFunc);

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::vector<HostAddr> addrs_;
    HostAddr active_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METACLIENT_H_
