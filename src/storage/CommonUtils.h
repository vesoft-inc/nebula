/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/IndexManager.h"
#include "common/base/ConcurrentLRUCache.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "codec/RowReader.h"
#include "kvstore/KVStore.h"
#include "utils/MemoryLockWrapper.h"
#include <folly/concurrency/ConcurrentHashMap.h>


namespace nebula {
namespace storage {

struct ProcessorCounters {
    stats::CounterId numCalls_;
    stats::CounterId numErrors_;
    stats::CounterId latency_;

    virtual ~ProcessorCounters() = default;

    virtual void init(const std::string& counterName) {
        if (!numCalls_.valid()) {
            numCalls_ = stats::StatsManager::registerStats("num_" + counterName,
                                                           "rate, sum");
            numErrors_ = stats::StatsManager::registerStats("num_" + counterName + "_errors",
                                                            "rate, sum");
            latency_ = stats::StatsManager::registerHisto(counterName + "_latency_us",
                                                          1000,
                                                          0,
                                                          20000,
                                                          "avg, p75, p95, p99");
            VLOG(1) << "Succeeded in initializing the ProcessorCounters instance";
        } else {
            VLOG(1) << "ProcessorCounters instance has been initialized";
        }
    }
};


enum class IndexState {
    STARTING,  // The part is begin to build index.
    BUILDING,  // The part is building index.
    LOCKED,    // When the minor table is less than threshold.
               // we will refuse the write operation.
    FINISHED,  // The part is building index successfully.
};

using VertexCache = ConcurrentLRUCache<std::pair<VertexID, TagID>, std::string>;
using IndexKey    = std::tuple<GraphSpaceID, PartitionID>;
using IndexGuard  = folly::ConcurrentHashMap<IndexKey, IndexState>;

using VMLI = std::tuple<GraphSpaceID, PartitionID, TagID, VertexID>;
using EMLI = std::tuple<GraphSpaceID, PartitionID, VertexID, EdgeType, EdgeRanking, VertexID>;
using VerticesMemLock = MemoryLockCore<VMLI>;
using EdgesMemLock = MemoryLockCore<EMLI>;

class TransactionManager;

// unify TagID, EdgeType
using SchemaID = TagID;
static_assert(sizeof(SchemaID) == sizeof(EdgeType), "sizeof(TagID) != sizeof(EdgeType)");

class StorageEnv {
public:
    kvstore::KVStore*                               kvstore_{nullptr};
    meta::SchemaManager*                            schemaMan_{nullptr};
    meta::IndexManager*                             indexMan_{nullptr};
    std::atomic<int32_t>                            onFlyingRequest_{0};
    std::unique_ptr<IndexGuard>                     rebuildIndexGuard_{nullptr};
    meta::MetaClient*                               metaClient_{nullptr};
    TransactionManager*                             txnMan_{nullptr};
    std::unique_ptr<VerticesMemLock>                verticesML_{nullptr};
    std::unique_ptr<EdgesMemLock>                   edgesML_{nullptr};


    IndexState getIndexState(GraphSpaceID space, PartitionID part) {
        auto key = std::make_tuple(space, part);
        auto iter = rebuildIndexGuard_->find(key);
        if (iter != rebuildIndexGuard_->cend()) {
            return iter->second;
        }
        return IndexState::FINISHED;
    }

    bool checkRebuilding(IndexState indexState) {
        return indexState == IndexState::BUILDING;
    }

    bool checkIndexLocked(IndexState indexState) {
        return indexState == IndexState::LOCKED;
    }
};

class IndexCountWrapper {
public:
    explicit IndexCountWrapper(StorageEnv* env)
        : count_(&env->onFlyingRequest_) {
        count_->fetch_add(1, std::memory_order_release);
    }

    IndexCountWrapper(const IndexCountWrapper&) = delete;

    IndexCountWrapper(IndexCountWrapper&& icw) noexcept
        : count_(icw.count_) {
        count_->fetch_add(1, std::memory_order_release);
    }

    IndexCountWrapper& operator=(const IndexCountWrapper&) = delete;

    IndexCountWrapper& operator=(IndexCountWrapper&& icw) noexcept {
        if (this != &icw) {
            count_ = icw.count_;
            count_->fetch_add(1, std::memory_order_release);
        }
        return *this;
    }

    ~IndexCountWrapper() {
        count_->fetch_sub(1, std::memory_order_release);
    }

private:
    std::atomic<int32_t>* count_;
};

enum class ResultStatus {
    NORMAL = 0,
    ILLEGAL_DATA = -1,
    FILTER_OUT = -2,
};

struct PropContext;

// PlanContext stores information **unchanged** during the process.
// All processor won't change them after request is parsed.
class PlanContext {
public:
    PlanContext(StorageEnv* env, GraphSpaceID spaceId, size_t vIdLen, bool isIntId)
        : env_(env), spaceId_(spaceId), vIdLen_(vIdLen), isIntId_(isIntId) {}

    StorageEnv*         env_;
    GraphSpaceID        spaceId_;
    size_t              vIdLen_;
    bool                isIntId_;

    // used in lookup only
    bool                isEdge_ = false;

    // used for toss version
    int64_t             defaultEdgeVer_ = 0L;

    // Manage expressions
    ObjectPool          objPool_;
};

// RunTimeContext stores information **may changed** during the process. Since not all processor use
// all following fields, just list all of them here.
// todo(doodle): after filter is pushed down, I believe all field will not be changed anymore during
// process
struct RunTimeContext {
    explicit RunTimeContext(PlanContext* planContext) : planContext_(planContext) {}

    StorageEnv* env() const {
        return planContext_->env_;
    }

    GraphSpaceID spaceId() const {
        return planContext_->spaceId_;
    }

    size_t vIdLen() const {
        return planContext_->vIdLen_;
    }

    bool isIntId() const {
        return planContext_->isIntId_;
    }

    bool isEdge() const {
        return planContext_->isEdge_;
    }

    ObjectPool* objPool() {
        return &planContext_->objPool_;
    }

    PlanContext                        *planContext_;
    TagID                               tagId_ = 0;
    std::string                         tagName_ = "";
    const meta::NebulaSchemaProvider   *tagSchema_{nullptr};

    EdgeType                            edgeType_ = 0;
    std::string                         edgeName_ = "";
    const meta::NebulaSchemaProvider   *edgeSchema_{nullptr};

    // used for GetNeighbors
    size_t                              columnIdx_ = 0;
    const std::vector<PropContext>*     props_ = nullptr;

    // used for update
    bool                                insert_ = false;

    ResultStatus                        resultStat_{ResultStatus::NORMAL};
};

class CommonUtils final {
public:
    static bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                       RowReader* reader,
                                       const std::string& ttlCol,
                                       int64_t ttlDuration);

    static bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                       const Value& v,
                                       const std::string& ttlCol,
                                       int64_t ttlDuration);

    static std::pair<bool, std::pair<int64_t, std::string>>
        ttlProps(const meta::SchemaProviderIf* schema);

    static StatusOr<Value> ttlValue(const meta::SchemaProviderIf* schema, RowReader* reader);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_
