/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "common/base/Base.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/IndexManager.h"
#include "common/base/ConcurrentLRUCache.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "codec/RowReader.h"
#include "kvstore/KVStore.h"
#include <folly/concurrency/ConcurrentHashMap.h>

namespace nebula {
namespace storage {

enum class IndexState {
    STARTING,  // The part is begin to build index.
    BUILDING,  // The part is building index.
    LOCKED,    // When the minor table is less than threshold.
               // we will refuse the write operation.
    FINISHED,  // The part is building index successfully.
};

using VertexCache = ConcurrentLRUCache<std::pair<VertexID, TagID>, std::string>;
using IndexKey    = std::tuple<GraphSpaceID, IndexID, PartitionID>;
using IndexGuard  = folly::ConcurrentHashMap<IndexKey, IndexState>;


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

    bool checkRebuilding(GraphSpaceID space, PartitionID part, IndexID indexID) {
        auto key = std::make_tuple(space, indexID, part);
        auto iter = rebuildIndexGuard_->find(key);
        return iter != rebuildIndexGuard_->cend() && iter->second == IndexState::BUILDING;
    }

    bool checkIndexLocked(GraphSpaceID space, PartitionID part, IndexID indexID) {
        auto key = std::make_tuple(space, indexID, part);
        auto iter = rebuildIndexGuard_->find(key);
        return iter != rebuildIndexGuard_->cend() && iter->second == IndexState::LOCKED;
    }
};

enum class ResultStatus {
    NORMAL = 0,
    ILLEGAL_DATA = -1,
    FILTER_OUT = -2,
};

struct PropContext;

// PlanContext stores some information during the process
class PlanContext {
public:
    PlanContext(StorageEnv* env, GraphSpaceID spaceId, size_t vIdLen, bool isIntId)
        : env_(env), spaceId_(spaceId), vIdLen_(vIdLen) , isIntId_(isIntId) {}

    StorageEnv*         env_;
    GraphSpaceID        spaceId_;
    size_t              vIdLen_;
    bool                isIntId_;

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

    // used for lookup
    bool                                isEdge_ = false;
};

class CommonUtils final {
public:
    static bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                       RowReader* reader,
                                       const std::string& ttlCol,
                                       int64_t ttlDuration);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_
