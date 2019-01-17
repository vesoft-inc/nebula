/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaClient.h"
#include "thrift/ThriftClientManager.h"

namespace nebula {
namespace meta {


template<typename Request, typename RemoteFunc, typename Response>
Response MetaClient::collectResponse(Request req,
                                     RemoteFunc remoteFunc) {
    auto* evb = ioThreadPool_->getEventBase();
    auto client = thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>
                             ::getClient(active_, evb);
    folly::Promise<Response> pro;
    auto f = pro.getFuture();
    evb->runInEventBaseThread([&]() {
        remoteFunc(client, std::move(req)).then(evb, [&](folly::Try<Response>&& resp) {
            pro.setValue(resp.value());
        });
    });
    return std::move(f).get();
}

Status MetaClient::createNode(std::string path, std::string val) {
    cpp2::CreateNodeRequest req;
    req.set_path(std::move(path));
    req.set_value(std::move(val));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_createNode(request);
                });
    return handleResponse(resp);
}

Status MetaClient::setNode(std::string path, std::string val) {
    cpp2::SetNodeRequest req;
    req.set_path(std::move(path));
    req.set_value(std::move(val));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_setNode(request);
                });
    return handleResponse(resp);
}

StatusOr<std::string> MetaClient::getNode(std::string path) {
    cpp2::GetNodeRequest req;
    req.set_path(std::move(path));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getNode(request);
                });
    if (resp.get_code() == cpp2::ErrorCode::SUCCEEDED) {
        return std::move(resp.get_value());
    }
    return handleResponse(resp);
}

StatusOr<std::vector<std::string>> MetaClient::listChildren(std::string path) {
    cpp2::ListChildrenRequest req;
    req.set_path(std::move(path));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listChildren(request);
                });
    if (resp.get_code() == cpp2::ErrorCode::SUCCEEDED) {
        return std::move(resp.get_children());
    }
    return handleResponse(resp);
}

Status MetaClient::removeNode(std::string path) {
    cpp2::RemoveNodeRequest req;
    req.set_path(std::move(path));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeNode(request);
                });
    return handleResponse(resp);
}

template<typename RESP>
Status MetaClient::handleResponse(const RESP& resp) {
    switch (resp.get_code()) {
        case cpp2::ErrorCode::SUCCEEDED:
            return Status::OK();
        case cpp2::ErrorCode::E_LEADER_CHANGED:
            return Status::Error("Leader changed!");
        case cpp2::ErrorCode::E_NODE_EXISTED:
            return Status::Error("Node has been existed!");
        case cpp2::ErrorCode::E_NODE_NOT_FOUND:
            return Status::Error("Not found the node!");
        case cpp2::ErrorCode::E_INVALID_PATH:
            return Status::Error("Invalid path!");
        case cpp2::ErrorCode::E_CHILD_EXISTED:
            return Status::Error("Children existed!");
        default:
            return Status::Error("Unknown code %d", static_cast<int32_t>(resp.get_code()));
    }
}

}  // namespace meta
}  // namespace nebula
