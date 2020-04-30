/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_EXECUTIONCONTEXT_H_
#define SERVICE_EXECUTIONCONTEXT_H_

#include "base/Base.h"

#include <unordered_map>

#include "cpp/helpers.h"
#include "meta/SchemaManager.h"
#include "parser/SequentialSentences.h"
#include "service/RequestContext.h"
// #include "meta/ClientBasedGflagsManager.h"
#include "clients/meta/MetaClient.h"
#include "charset/Charset.h"
#include "util/ObjectPool.h"

/**
 * ExecutionContext holds context infos in the execution process, e.g. clients of storage or meta services.
 */

namespace nebula {

namespace storage {
class GraphStorageClient;
}   // namespace storage

namespace graph {

class ExecutionContext final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;

    ExecutionContext(RequestContextPtr rctx,
                     meta::SchemaManager* sm,
                     // meta::ClientBasedGflagsManager *gflagsManager,
                     storage::GraphStorageClient* storage,
                     meta::MetaClient* metaClient,
                     CharsetInfo* charsetInfo)
        : rctx_(std::move(rctx)),
          sm_(sm),
          // gflagsManager_(gflagsManager),
          storageClient_(storage),
          metaClient_(metaClient),
          charsetInfo_(charsetInfo),
          objPool_(std::make_unique<ObjectPool>()) {
        DCHECK_NOTNULL(sm_);
        DCHECK_NOTNULL(storageClient_);
        DCHECK_NOTNULL(metaClient_);
        DCHECK_NOTNULL(charsetInfo_);
    }

    // for test
    ExecutionContext() : objPool_(std::make_unique<ObjectPool>()) {}

    ~ExecutionContext();

    void addValue(const std::string& name, Value&& value) {
        auto iter = valuesMap_.find(name);
        if (iter != valuesMap_.end()) {
            iter->second.emplace_back(std::move(value));
        } else {
            auto res = valuesMap_.emplace(name, std::list<Value>{std::move(value)});
            DCHECK(res.second);
        }
    }

    const Value& getValue(const std::string& name) const {
        auto iter = valuesMap_.find(name);
        DCHECK(iter != valuesMap_.end());
        return iter->second.back();
    }

    size_t numVersions(const std::string& name) const {
        auto iter = valuesMap_.find(name);
        DCHECK(iter != valuesMap_.end());
        return iter->second.size();
    }

    const std::list<Value>& getHistory(const std::string& name) const {
        auto iter = valuesMap_.find(name);
        DCHECK(iter != valuesMap_.end());
        return iter->second;
    }

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

    CharsetInfo* getCharsetInfo() const {
        return charsetInfo_;
    }

    ObjectPool* objPool() const {
        return objPool_.get();
    }

private:
    RequestContextPtr                           rctx_;
    meta::SchemaManager                        *sm_{nullptr};
    // meta::ClientBasedGflagsManager             *gflagsManager_{nullptr};
    storage::GraphStorageClient                *storageClient_{nullptr};
    meta::MetaClient                           *metaClient_{nullptr};
    CharsetInfo                                *charsetInfo_{nullptr};

    std::unique_ptr<ObjectPool> objPool_;
    // It will be move to QueryContex as the result store of the execution
    std::unordered_map<std::string, std::list<Value>> valuesMap_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTIONCONTEXT_H_
