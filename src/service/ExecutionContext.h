/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_EXECUTIONCONTEXT_H_
#define SERVICE_EXECUTIONCONTEXT_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "service/RequestContext.h"
#include "parser/SequentialSentences.h"
#include "meta/SchemaManager.h"
// #include "meta/ClientBasedGflagsManager.h"
#include "clients/meta/MetaClient.h"
#include "clients/storage/GraphStorageClient.h"

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
                     // meta::ClientBasedGflagsManager *gflagsManager,
                     storage::GraphStorageClient *storage,
                     meta::MetaClient *metaClient) {
        rctx_ = std::move(rctx);
        sm_ = sm;
        // gflagsManager_ = gflagsManager;
        storageClient_ = storage;
        metaClient_ = metaClient;
    }

    ~ExecutionContext();

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    meta::SchemaManager* schemaManager() const {
        return sm_;
    }

    storage::GraphStorageClient* getStorageClient() const {
        return storageClient_;
    }

    meta::MetaClient* getMetaClient() const {
        return metaClient_;
    }

private:
    RequestContextPtr                           rctx_;
    meta::SchemaManager                        *sm_{nullptr};
    // meta::ClientBasedGflagsManager             *gflagsManager_{nullptr};
    storage::GraphStorageClient                *storageClient_{nullptr};
    meta::MetaClient                           *metaClient_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTIONCONTEXT_H_
