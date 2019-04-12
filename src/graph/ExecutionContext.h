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
#include "parser/SequentialSentences.h"
#include "meta/SchemaManager.h"
#include "graph/VariableHolder.h"

/**
 * ExecutionContext holds context infos in the execution process, e.g. clients of storage or meta services.
 */

namespace nebula {
namespace storage {
class StorageClient;
}   // namespace storage
namespace graph {

class ExecutionContext final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;
    ExecutionContext(RequestContextPtr rctx,
                     meta::SchemaManager *sm,
                     storage::StorageClient *storage) {
        rctx_ = std::move(rctx);
        sm_ = sm;
        storage_ = storage;
        variableHolder_ = std::make_unique<VariableHolder>();
    }

    ~ExecutionContext() = default;

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    meta::SchemaManager* schemaManager() const {
        return sm_;
    }

    storage::StorageClient* storage() const {
        return storage_;
    }

    VariableHolder* variableHolder() const {
        return variableHolder_.get();
    }

private:
    RequestContextPtr                           rctx_;
    meta::SchemaManager                              *sm_{nullptr};
    storage::StorageClient                     *storage_{nullptr};
    std::unique_ptr<VariableHolder>             variableHolder_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTIONCONTEXT_H_
