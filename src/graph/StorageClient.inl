/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "thrift/ThriftClientManager.h"
#include <folly/Try.h>

namespace nebula {
namespace graph {

using namespace storage;


template<class Request, class RemoteFunc, class RPCFailureHandler>
folly::SemiFuture<storage::cpp2::ExecResponse> StorageClient::collectExecResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RPCFailureHandler&& rpcFailureHandler) {
    struct ExecResponseContext {
    private:
        std::mutex lock_;
        std::unordered_map<HostAddr, Request> ongoingRequests_;
        bool finishSending_{false};
        bool fulfilled_{false};

        using Iterator = typename decltype(ongoingRequests_)::iterator;

    public:
        folly::Promise<cpp2::ExecResponse> promise;
        std::vector<cpp2::ResultCode> results;
        int32_t maxLatency{0};
        RemoteFunc serverMethod;
        RPCFailureHandler rpcFailure;

        ExecResponseContext(RemoteFunc&& func, RPCFailureHandler&& handler)
            : serverMethod(std::move(func))
            , rpcFailure(std::move(handler)) {}

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

        std::pair<Iterator, bool> insertRequest(HostAddr host, Request&& req) {
            std::lock_guard<std::mutex> g(lock_);
            return ongoingRequests_.emplace(host, std::move(req));
        }

        Iterator findRequest(HostAddr host) {
            std::lock_guard<std::mutex> g(lock_);
            auto it = ongoingRequests_.find(host);
            DCHECK(it != ongoingRequests_.end());
            return it;
        }

        // Return true if processed all responses
        bool removeRequest(Iterator it) {
            std::lock_guard<std::mutex> g(lock_);
            ongoingRequests_.erase(it);
            if (finishSending_ && !fulfilled_ && ongoingRequests_.empty()) {
                fulfilled_ = true;
                return true;
            } else {
                return false;
            }
        }
    };

    auto context = std::make_shared<ExecResponseContext>(
        std::move(remoteFunc),
        std::move(rpcFailureHandler));

    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }

    for (auto& req : requests) {
        auto& host = req.first;
        auto client = thrift::ThriftClientManager<cpp2::StorageServiceAsyncClient>
                            ::getClient(host, evb);
        auto res = context->insertRequest(host, std::move(req.second));
        DCHECK(res.second);
        context->serverMethod(client, res.first->second)
            // Future process code will be executed on the IO thread
            .then(evb, [context, host] (folly::Try<cpp2::ExecResponse>&& val) {
                auto it = context->findRequest(host);
                if (val.hasException()) {
                    std::vector<cpp2::ResultCode> results;
                    context->rpcFailure(host, it->second, results);
                    context->results.insert(context->results.end(),
                                            results.begin(),
                                            results.end());
                } else {
                    auto value = std::move(val.value());
                    for (auto& code : value.get_codes()) {
                        if (code.get_code() == cpp2::ErrorCode::E_LEADER_CHANGED) {
                            // TODO Need to retry the new leader
                            LOG(FATAL) << "Not implmented";
                        } else {
                            // Simply keep the result
                            context->results.emplace_back(std::move(code));
                        }
                    }

                    if (value.get_latency_in_ms() > context->maxLatency) {
                        context->maxLatency = value.get_latency_in_ms();
                    }
                }

                if (context->removeRequest(it)) {
                    // Received all responses
                    cpp2::ExecResponse resp;
                    resp.set_codes(std::move(context->results));
                    resp.set_latency_in_ms(context->maxLatency);
                    context->promise.setValue(std::move(resp));
                }
            });
    }

    if (context->finishSending()) {
        // Received all responses, most likely, all rpc failed
        cpp2::ExecResponse resp;
        resp.set_codes(std::move(context->results));
        resp.set_latency_in_ms(context->maxLatency);
        context->promise.setValue(std::move(resp));
    }

    return context->promise.getSemiFuture();
}


template<class Request, class RemoteFunc, class RPCFailureHandler>
folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::collectQueryResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RPCFailureHandler&& rpcFailureHandler) {
    struct QueryResponseContext {
    private:
        std::mutex lock_;
        std::unordered_map<HostAddr, Request> ongoingRequests_;
        bool finishSending_{false};
        bool fulfilled_{false};

        using Iterator = typename decltype(ongoingRequests_)::iterator;

    public:
        folly::Promise<cpp2::QueryResponse> promise;
        std::vector<cpp2::ResultCode> results;
        std::vector<cpp2::VertexResponse> data;
        std::shared_ptr<cpp2::Schema> vertexSchema;
        std::shared_ptr<cpp2::Schema> edgeSchema;
        int32_t maxLatency{0};

        RemoteFunc serverMethod;
        RPCFailureHandler rpcFailure;

        QueryResponseContext(RemoteFunc&& func, RPCFailureHandler&& handler)
            : serverMethod(std::move(func))
            , rpcFailure(std::move(handler)) {}

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

        std::pair<Iterator, bool> insertRequest(HostAddr host, Request&& req) {
            std::lock_guard<std::mutex> g(lock_);
            return ongoingRequests_.emplace(host, std::move(req));
        }

        Iterator findRequest(HostAddr host) {
            std::lock_guard<std::mutex> g(lock_);
            auto it = ongoingRequests_.find(host);
            DCHECK(it != ongoingRequests_.end());
            return it;
        }

        // Return true if processed all responses
        bool removeRequest(Iterator it) {
            std::lock_guard<std::mutex> g(lock_);
            ongoingRequests_.erase(it);
            if (finishSending_ && !fulfilled_ && ongoingRequests_.empty()) {
                fulfilled_ = true;
                return true;
            } else {
                return false;
            }
        }
    };

    auto context = std::make_shared<QueryResponseContext>(
        std::move(remoteFunc),
        std::move(rpcFailureHandler));

    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }

    for (auto& req : requests) {
        auto& host = req.first;
        auto client = thrift::ThriftClientManager<cpp2::StorageServiceAsyncClient>
                            ::getClient(host, evb);
        auto res = context->insertRequest(host, std::move(req.second));
        DCHECK(res.second);
        context->serverMethod(client, res.first->second)
            // Future process code will be executed on the IO thread
            .then(evb, [context, host] (folly::Try<cpp2::QueryResponse>&& val) {
                auto it = context->findRequest(host);
                if (val.hasException()) {
                    std::vector<cpp2::ResultCode> results;
                    context->rpcFailure(host, it->second, results);
                    context->results.insert(context->results.end(),
                                            results.begin(),
                                            results.end());
                } else {
                    auto value = std::move(val.value());
                    for (auto& code : value.get_codes()) {
                        if (code.get_code() == cpp2::ErrorCode::E_LEADER_CHANGED) {
                            // TODO Need to retry the new leader
                            LOG(FATAL) << "Not implmented";
                        } else {
                            // Simply keep the result
                            context->results.emplace_back(std::move(code));
                        }
                    }

                    // Adjust the latency
                    if (value.get_latency_in_ms() > context->maxLatency) {
                        context->maxLatency = value.get_latency_in_ms();
                    }

                    // Vertex schema
                    if (!context->vertexSchema) {
                        auto* schema = value.get_vertex_schema();
                        if (schema != nullptr) {
                            context->vertexSchema = std::make_shared<cpp2::Schema>(*schema);
                        }
                    }

                    // edge schema
                    if (!context->edgeSchema) {
                        auto* schema = value.get_edge_schema();
                        if (schema != nullptr) {
                            context->edgeSchema = std::make_shared<cpp2::Schema>(*schema);
                        }
                    }

                    // data
                    auto* data = value.get_vertices();
                    if (data != nullptr) {
                        context->data.insert(context->data.end(),
                                             data->begin(),
                                             data->end());
                    }
                }

                if (context->removeRequest(it)) {
                    // Received all responses
                    cpp2::QueryResponse resp;
                    resp.set_codes(std::move(context->results));
                    resp.set_latency_in_ms(context->maxLatency);
                    if (!!context->vertexSchema) {
                        resp.set_vertex_schema(std::move(*(context->vertexSchema)));
                    }
                    if (!!context->edgeSchema) {
                        resp.set_edge_schema(std::move(*(context->edgeSchema)));
                    }
                    if (!context->data.empty()) {
                        resp.set_vertices(std::move(context->data));
                    }
                    context->promise.setValue(std::move(resp));
                }
            });
    }

    if (context->finishSending()) {
        // Received all responses, most likely, all rpc failed
        cpp2::QueryResponse resp;
        resp.set_codes(std::move(context->results));
        resp.set_latency_in_ms(context->maxLatency);
        if (!!context->vertexSchema) {
            resp.set_vertex_schema(std::move(*(context->vertexSchema)));
        }
        if (!!context->edgeSchema) {
            resp.set_edge_schema(std::move(*(context->edgeSchema)));
        }
        if (!context->data.empty()) {
            resp.set_vertices(std::move(context->data));
        }
        context->promise.setValue(std::move(resp));
    }

    return context->promise.getSemiFuture();
}


}   // namespace graph
}   // namespace nebula

