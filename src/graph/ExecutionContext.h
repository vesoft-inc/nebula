/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_EXECUTIONCONTEXT_H_
#define GRAPH_EXECUTIONCONTEXT_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "graph/RequestContext.h"
#include "parser/CompoundSentence.h"
#include "graph/mock/SchemaManager.h"
#include "graph/mock/StorageService.h"

/**
 * ExecutionContext holds context infos in the execution process, e.g. clients of storage or meta services.
 */

namespace nebula {
namespace graph {

class ExecutionContext final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;
    ExecutionContext(RequestContextPtr rctx, SchemaManager *sm, StorageService *storage) {
        rctx_ = std::move(rctx);
        sm_ = sm;
        storage_ = storage;
    }

    ~ExecutionContext() = default;

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    SchemaManager* schemaManager() const {
        return sm_;
    }

    StorageService* storage() const {
        return storage_;
    }

private:
    RequestContextPtr                           rctx_;
    SchemaManager                              *sm_{nullptr};
    StorageService                             *storage_{nullptr};
};

}   // namespace graph
}   // namespace graph

#endif  // GRAPH_EXECUTIONCONTEXT_H_
