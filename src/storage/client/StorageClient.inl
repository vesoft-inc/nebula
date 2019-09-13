/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Try.h>

namespace nebula {
namespace storage {

namespace {

template<class Request, class RemoteFunc, class Response>
struct ResponseContext {
public:
    ResponseContext(size_t reqsSent, RemoteFunc&& remoteFunc)
        : resp(reqsSent)
        , serverMethod(std::move(remoteFunc)) {}

    // Return true if processed all responses
    bool finishSending() {
        std::lock_guard<std::mutex> g(lock_);
        finishSending_ = true;
        if (ongoingRequests_.empty() && !fulfilled_) {
            fulfilled_ = true;
            return true;
        } else {
            return false;
        }
    }

    std::pair<const Request*, bool> insertRequest(HostAddr host, Request&& req) {
        std::lock_guard<std::mutex> g(lock_);
        auto res = ongoingRequests_.emplace(host, std::move(req));
        return std::make_pair(&res.first->second, res.second);
    }

    const Request& findRequest(HostAddr host) {
        std::lock_guard<std::mutex> g(lock_);
        auto it = ongoingRequests_.find(host);
        DCHECK(it != ongoingRequests_.end());
        return it->second;
    }

    // Return true if processed all responses
    bool removeRequest(HostAddr host) {
        std::lock_guard<std::mutex> g(lock_);
        ongoingRequests_.erase(host);
        if (finishSending_ && !fulfilled_ && ongoingRequests_.empty()) {
            fulfilled_ = true;
            return true;
        } else {
            return false;
        }
    }

public:
    folly::Promise<StorageRpcResponse<Response>> promise;
    StorageRpcResponse<Response> resp;
    RemoteFunc serverMethod;

private:
    std::mutex lock_;
    std::unordered_map<HostAddr, Request> ongoingRequests_;
    bool finishSending_{false};
    bool fulfilled_{false};
};

}  // Anonymous namespace


template<class Request, class RemoteFunc, class Response>
folly::SemiFuture<StorageRpcResponse<Response>> StorageClient::collectResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc) {
    auto context = std::make_shared<ResponseContext<Request, RemoteFunc, Response>>(
        requests.size(), std::move(remoteFunc));

    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }

    for (auto& req : requests) {
        auto& host = req.first;
        auto spaceId = req.second.get_space_id();
        auto res = context->insertRequest(host, std::move(req.second));
        DCHECK(res.second);
        // Invoke the remote method
        folly::via(evb, [this, evb, context, host, spaceId, res] () mutable {
            auto client = clientsMan_->client(host, evb);
            // Result is a pair of <Request&, bool>
            context->serverMethod(client.get(), *res.first)
            // Future process code will be executed on the IO thread
            // Since all requests are sent using the same eventbase, all then-callback
            // will be executed on the same IO thread
            .then(evb, [this, context, host, spaceId] (folly::Try<Response>&& val) {
                auto& r = context->findRequest(host);
                if (val.hasException()) {
                    LOG(ERROR) << "Request to " << host << " failed: " << val.exception().what();
                    for (auto& part : r.parts) {
                        VLOG(3) << "Exception! Failed part " << part.first;
                        context->resp.failedParts().emplace(
                            part.first,
                            storage::cpp2::ErrorCode::E_RPC_FAILURE);
                        invalidLeader(spaceId, part.first);
                    }
                    context->resp.markFailure();
                } else {
                    auto resp = std::move(val.value());
                    auto& result = resp.get_result();
                    bool hasFailure{false};
                    for (auto& code : result.get_failed_codes()) {
                        VLOG(3) << "Failure! Failed part " << code.get_part_id()
                                << ", failed code " << static_cast<int32_t>(code.get_code());
                        hasFailure = true;
                        if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                            auto* leader = code.get_leader();
                            if (leader != nullptr
                                    && leader->get_ip() != 0
                                    && leader->get_port() != 0) {
                                updateLeader(spaceId,
                                             code.get_part_id(),
                                             HostAddr(leader->get_ip(), leader->get_port()));
                            }
                        } else {
                            // Simply keep the result
                            context->resp.failedParts().emplace(code.get_part_id(),
                                                                code.get_code());
                        }
                    }
                    if (hasFailure) {
                        context->resp.markFailure();
                    }

                    // Adjust the latency
                    context->resp.setLatency(result.get_latency_in_us());

                    // Keep the response
                    context->resp.responses().emplace_back(std::move(resp));
                }

                if (context->removeRequest(host)) {
                    // Received all responses
                    context->promise.setValue(std::move(context->resp));
                }
            });
        });  // via
    }  // for
    if (context->finishSending()) {
        // Received all responses, most likely, all rpc failed
        context->promise.setValue(std::move(context->resp));
    }

    return context->promise.getSemiFuture();
}


template<class Request, class RemoteFunc, class Response>
folly::Future<StatusOr<Response>> StorageClient::getResponse(
        folly::EventBase* evb,
        std::pair<HostAddr, Request> request,
        RemoteFunc remoteFunc) {
    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }
    folly::Promise<StatusOr<Response>> pro;
    auto f = pro.getFuture();
    folly::via(evb, [evb, request = std::move(request), remoteFunc = std::move(remoteFunc),
                     pro = std::move(pro), this] () mutable {
        auto host = request.first;
        auto client = clientsMan_->client(host, evb);
        auto spaceId = request.second.get_space_id();
        LOG(INFO) << "Send request to storage " << host;
        remoteFunc(client.get(), std::move(request.second))
             .then(evb, [spaceId, p = std::move(pro), this] (folly::Try<Response>&& t) mutable {
            // exception occurred during RPC
            if (t.hasException()) {
                p.setValue(Status::Error(folly::stringPrintf("RPC failure in StorageClient: %s",
                                                             t.exception().what().c_str())));
                return;
            }
            auto&& resp = std::move(t.value());
            // leader changed
            auto& result = resp.get_result();
            for (auto& code : result.get_failed_codes()) {
                VLOG(3) << "Failure! Failed part " << code.get_part_id()
                        << ", failed code " << static_cast<int32_t>(code.get_code());
                if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                    auto* leader = code.get_leader();
                    if (leader != nullptr && leader->get_ip() != 0 && leader->get_port() != 0) {
                        updateLeader(spaceId, code.get_part_id(),
                                     HostAddr(leader->get_ip(), leader->get_port()));
                    }
                }
            }
            p.setValue(std::move(resp));
        });
    });  // via
    return f;
}

}   // namespace storage
}   // namespace nebula

