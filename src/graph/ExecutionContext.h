/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTIONCONTEXT_H_
#define GRAPH_EXECUTIONCONTEXT_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "graph/RequestContext.h"
#include "parser/SequentialSentences.h"
#include "meta/SchemaManager.h"
#include "meta/ClientBasedGflagsManager.h"
#include "graph/VariableHolder.h"
#include "meta/client/MetaClient.h"
#include "charset/Charset.h"

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
                     meta::ClientBasedGflagsManager *gflagsManager,
                     storage::StorageClient *storage,
                     meta::MetaClient *metaClient,
                     CharsetInfo* charsetInfo) {
        rctx_ = std::move(rctx);
        sm_ = sm;
        gflagsManager_ = gflagsManager;
        storageClient_ = storage;
        metaClient_ = metaClient;
        variableHolder_ = std::make_unique<VariableHolder>(rctx_->session());
        charsetInfo_ = charsetInfo;
    }

    ~ExecutionContext();

    RequestContext<cpp2::ExecutionResponse>* rctx() const {
        return rctx_.get();
    }

    meta::SchemaManager* schemaManager() const {
        return sm_;
    }

    meta::ClientBasedGflagsManager* gflagsManager() const {
        return gflagsManager_;
    }

    storage::StorageClient* getStorageClient() const {
        return storageClient_;
    }

    VariableHolder* variableHolder() const {
        return variableHolder_.get();
    }

    meta::MetaClient* getMetaClient() const {
        return metaClient_;
    }

    CharsetInfo* getCharsetInfo() const {
        return charsetInfo_;
    }

    void addWarningMsg(std::string msg) {
        warnMsgs_.emplace_back(std::move(msg));
    }

    const std::vector<std::string>& getWarningMsg() const {
        return warnMsgs_;
    }

private:
    RequestContextPtr                           rctx_;
    meta::SchemaManager                        *sm_{nullptr};
    meta::ClientBasedGflagsManager             *gflagsManager_{nullptr};
    storage::StorageClient                     *storageClient_{nullptr};
    meta::MetaClient                           *metaClient_{nullptr};
    std::unique_ptr<VariableHolder>             variableHolder_;
    CharsetInfo                                *charsetInfo_{nullptr};
    std::vector<std::string>                    warnMsgs_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTIONCONTEXT_H_
