/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_QUERYCONTEXT_H_
#define CONTEXT_QUERYCONTEXT_H_

#include "common/base/ObjectPool.h"
#include "common/charset/Charset.h"
#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/cpp/helpers.h"
#include "common/datatypes/Value.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "context/ExecutionContext.h"
#include "context/Symbols.h"
#include "context/ValidateContext.h"
#include "parser/SequentialSentences.h"
#include "service/RequestContext.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe
 *
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/
class QueryContext {
public:
    using RequestContextPtr = std::unique_ptr<RequestContext<ExecutionResponse>>;

    QueryContext();
    QueryContext(RequestContextPtr rctx,
                 meta::SchemaManager* sm,
                 meta::IndexManager* im,
                 storage::GraphStorageClient* storage,
                 meta::MetaClient* metaClient,
                 CharsetInfo* charsetInfo);

    virtual ~QueryContext() = default;

    void setRCtx(RequestContextPtr rctx) {
        rctx_ = std::move(rctx);
    }

    void setSchemaManager(meta::SchemaManager* sm) {
        sm_ = sm;
    }

    void setIndexManager(meta::IndexManager* im) {
        im_ = im;
    }

    void setStorageClient(storage::GraphStorageClient* storage) {
        storageClient_ = storage;
    }

    void setMetaClient(meta::MetaClient* metaClient) {
        metaClient_ = metaClient;
    }

    void setCharsetInfo(CharsetInfo* charsetInfo) {
        charsetInfo_ = charsetInfo;
    }

    RequestContext<ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    ValidateContext* vctx() const {
        return vctx_.get();
    }

    ExecutionContext* ectx() const {
        return ectx_.get();
    }

    ExecutionPlan* plan() const {
        return ep_.get();
    }

    meta::SchemaManager* schemaMng() const {
        return sm_;
    }

    meta::IndexManager* indexMng() const {
        return im_;
    }

    storage::GraphStorageClient* getStorageClient() const {
        return storageClient_;
    }

    meta::MetaClient* getMetaClient() const {
        return metaClient_;
    }

    CharsetInfo* getCharsetInfo() const {
        return charsetInfo_;
    }

    ObjectPool* objPool() const {
        return objPool_.get();
    }

    int64_t genId() const {
        return idGen_->id();
    }

    SymbolTable* symTable() const {
        return symTable_.get();
    }

    void setPartialSuccess() {
        DCHECK(rctx_ != nullptr);
        rctx_->resp().errorCode = ErrorCode::E_PARTIAL_SUCCEEDED;
    }

    void markKilled() {
        killed_.exchange(true);
    }

    bool isKilled() const {
        return killed_.load();
    }

private:
    void init();

    RequestContextPtr                                       rctx_;
    std::unique_ptr<ValidateContext>                        vctx_;
    std::unique_ptr<ExecutionContext>                       ectx_;
    std::unique_ptr<ExecutionPlan>                          ep_;
    meta::SchemaManager*                                    sm_{nullptr};
    meta::IndexManager*                                     im_{nullptr};
    storage::GraphStorageClient*                            storageClient_{nullptr};
    meta::MetaClient*                                       metaClient_{nullptr};
    CharsetInfo*                                            charsetInfo_{nullptr};

    // The Object Pool holds all internal generated objects.
    // e.g. expressions, plan nodes, executors
    std::unique_ptr<ObjectPool>                             objPool_;
    std::unique_ptr<IdGenerator>                            idGen_;
    std::unique_ptr<SymbolTable>                            symTable_;

    std::atomic<bool>                                       killed_{false};
};

}   // namespace graph
}   // namespace nebula
#endif   // CONTEXT_QUERYCONTEXT_H_
