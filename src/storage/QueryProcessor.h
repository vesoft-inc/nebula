/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYPROCESSOR_H_
#define STORAGE_QUERYPROCESSOR_H_

#include "base/Base.h"
#include <folly/SpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class QueryBoundProcessor : public BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse> {
public:
    static QueryBoundProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryBoundProcessor(kvstore);
    }

    void process(const cpp2::GetNeighborsRequest& req) override {
        VLOG(1) << req.get_space_id();
    }

private:
    QueryBoundProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetNeighborsRequest, cpp2::QueryResponse>(kvstore) {}
};

class QueryStatsProcessor : public BaseProcessor<cpp2::NeighborsStatsRequest, cpp2::QueryResponse> {
public:
    static QueryStatsProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryStatsProcessor(kvstore);
    }

    void process(const cpp2::NeighborsStatsRequest& req) override {
        VLOG(1) << req.get_space_id();
    }

private:
    QueryStatsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::NeighborsStatsRequest, cpp2::QueryResponse>(kvstore) {}
};

class QueryVertexPropsProcessor
    : public BaseProcessor<cpp2::VertexPropRequest, cpp2::QueryResponse> {
public:
    static QueryVertexPropsProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryVertexPropsProcessor(kvstore);
    }

    void process(const cpp2::VertexPropRequest& req) override {
        VLOG(1) << req.get_space_id();
        // TODO
    }

private:
    QueryVertexPropsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::VertexPropRequest, cpp2::QueryResponse>(kvstore) {}
};

class QueryEdgePropsProcessor : public BaseProcessor<cpp2::EdgePropRequest, cpp2::QueryResponse> {
public:
    static QueryEdgePropsProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryEdgePropsProcessor(kvstore);
    }

    void process(const cpp2::EdgePropRequest& req) override {
        VLOG(1) << req.get_space_id();
        // TODO
    }

private:
    QueryEdgePropsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::EdgePropRequest, cpp2::QueryResponse>(kvstore) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYPROCESSOR_H_
