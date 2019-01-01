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

namespace detail {

template<class Request, class Response, class RemoteFunc, class RequestPartAccessor>
struct ContextBase {
private:
    std::mutex lock_;
    std::unordered_map<HostAddr, Request> ongoingRequests_;
    bool finishSending_{false};
    bool fulfilled_{false};

    using Iterator = typename decltype(ongoingRequests_)::iterator;

public:
    using ResponseType = Response;

public:
    ContextBase(RemoteFunc&& remoteFunc,
                RequestPartAccessor&& requestPartAccessor)
        : serverMethod(std::move(remoteFunc))
        , partAccessor(std::move(requestPartAccessor)) {}

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


public:
    folly::Promise<Response> promise;
    std::vector<cpp2::ResultCode> results;
    int32_t maxLatency{0};

    RemoteFunc serverMethod;
    RequestPartAccessor partAccessor;
};

}  // namespace detail


template<class Request,
         class Context,
         class SingleResponseHandler,
         class AllDoneHandler>
folly::SemiFuture<typename Context::ResponseType> StorageClient::collectResponse(
        folly::EventBase* evb,
        std::shared_ptr<Context> context,
        std::unordered_map<HostAddr, Request> requests,
        SingleResponseHandler&& singleRespHandler,
        AllDoneHandler&& allDoneHandler) {
    struct HandlerWrapper {
        HandlerWrapper(SingleResponseHandler&& singleRespHandler,
                       AllDoneHandler&& allDoneHandler)
            : singleResp(std::move(singleRespHandler))
            , allDone(std::move(allDoneHandler)) {}

        SingleResponseHandler singleResp;
        AllDoneHandler allDone;
    };

    auto handlers = std::make_shared<HandlerWrapper>(
        std::move(singleRespHandler),
        std::move(allDoneHandler));

    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }

    for (auto& req : requests) {
        auto& host = req.first;
        auto client = thrift::ThriftClientManager<cpp2::StorageServiceAsyncClient>
                            ::getClient(host, evb);
        // Result is a pair of <Iterator, bool>
        auto res = context->insertRequest(host, std::move(req.second));
        DCHECK(res.second);
        // Invoke the remote method
        context->serverMethod(client, res.first->second)
            // Future process code will be executed on the IO thread
            .then(evb,
                  [context, handlers, host] (folly::Try<typename Context::ResponseType>&& val) {
                auto it = context->findRequest(host);
                if (val.hasException()) {
                    context->partAccessor(it->second, [context] (PartitionID part) {
                        cpp2::ResultCode result;
                        result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                        result.set_part_id(part);
                        context->results.emplace_back(std::move(result));
                    });
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

                    // All other works needed to process the response
                    handlers->singleResp(context, host, value);
                }

                if (context->removeRequest(it)) {
                    // Received all responses
                    handlers->allDone(context);
                }
            });
    }

    if (context->finishSending()) {
        // Received all responses, most likely, all rpc failed
        handlers->allDone(context);
    }

    return context->promise.getSemiFuture();
}


template<class Request, class RemoteFunc, class RequestPartAccessor>
folly::SemiFuture<cpp2::ExecResponse> StorageClient::collectExecResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RequestPartAccessor&& partAccessor) {
    using ContextBase = detail::ContextBase<Request,
                                            cpp2::ExecResponse,
                                            RemoteFunc,
                                            RequestPartAccessor>;

    struct ExecResponseContext : public ContextBase {
    public:
        ExecResponseContext(RemoteFunc&& remoteFunc, RequestPartAccessor&& accessor)
            : ContextBase(std::move(remoteFunc), std::move(accessor)) {}
    };

    auto context = std::make_shared<ExecResponseContext>(std::move(remoteFunc),
                                                         std::move(partAccessor));

    return collectResponse(
        evb, context, std::move(requests),
        // Single response handler
        [] (std::shared_ptr<ExecResponseContext>, HostAddr, cpp2::ExecResponse&) {},
        // All done handler
        [] (std::shared_ptr<ExecResponseContext> c) {
            cpp2::ExecResponse resp;
            resp.set_codes(std::move(c->results));
            resp.set_latency_in_ms(c->maxLatency);
            c->promise.setValue(std::move(resp));
        });
}


template<class Request, class RemoteFunc, class RequestPartAccessor>
folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::collectQueryResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RequestPartAccessor&& partAccessor) {
    using ContextBase = detail::ContextBase<Request,
                                            cpp2::QueryResponse,
                                            RemoteFunc,
                                            RequestPartAccessor>;

    struct QueryResponseContext : public ContextBase {
    public:
        QueryResponseContext(RemoteFunc&& remoteFunc, RequestPartAccessor&& accessor)
            : ContextBase(std::move(remoteFunc), std::move(accessor)) {}

    public:
        std::vector<cpp2::VertexResponse> data;
        std::shared_ptr<cpp2::Schema> vertexSchema;
        std::shared_ptr<cpp2::Schema> edgeSchema;
    };

    auto context = std::make_shared<QueryResponseContext>(std::move(remoteFunc),
                                                          std::move(partAccessor));

    return collectResponse(
        evb, context, std::move(requests),
        // Single response handler
        [] (std::shared_ptr<QueryResponseContext> c, HostAddr h, cpp2::QueryResponse& r) {
            UNUSED(h);

            // Vertex schema, only need to add once
            if (!c->vertexSchema) {
                auto* schema = r.get_vertex_schema();
                if (schema != nullptr) {
                    c->vertexSchema = std::make_shared<cpp2::Schema>(*schema);
                }
            }

            // edge schema, only need to add once
            if (!c->edgeSchema) {
                auto* schema = r.get_edge_schema();
                if (schema != nullptr) {
                    c->edgeSchema = std::make_shared<cpp2::Schema>(*schema);
                }
            }

            // data
            auto* data = r.get_vertices();
            if (data != nullptr) {
                c->data.insert(c->data.end(), data->begin(), data->end());
            }
        },
        // All done handler
        [] (std::shared_ptr<QueryResponseContext> c) {
            cpp2::QueryResponse resp;
            resp.set_codes(std::move(c->results));
            resp.set_latency_in_ms(c->maxLatency);
            if (!!c->vertexSchema) {
                resp.set_vertex_schema(std::move(*(c->vertexSchema)));
            }
            if (!!c->edgeSchema) {
                resp.set_edge_schema(std::move(*(c->edgeSchema)));
            }
            if (!c->data.empty()) {
                resp.set_vertices(std::move(c->data));
            }
            c->promise.setValue(std::move(resp));
        });
}


}   // namespace graph
}   // namespace nebula

