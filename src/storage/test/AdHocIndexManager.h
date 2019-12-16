/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADHOCINDEXMANAGER_H_
#define META_ADHOCINDEXMANAGER_H_

#include "meta/IndexManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace storage {

using TagIndexTuple  = std::tuple<TagID, IndexID, meta::cpp2::IndexFields>;
using EdgeIndexTuple = std::tuple<EdgeType, IndexID, meta::cpp2::IndexFields>;

class AdHocIndexManager final : public nebula::meta::IndexManager {
public:
    AdHocIndexManager() = default;
    ~AdHocIndexManager() = default;

    void addTagIndex(GraphSpaceID space,
                     IndexID indexID,
                     TagID tagID,
                     meta::cpp2::IndexFields fields);

    void addEdgeIndex(GraphSpaceID space,
                      IndexID indexID,
                      EdgeType edgeType,
                      meta::cpp2::IndexFields fields);

    StatusOr<std::shared_ptr<std::pair<TagID, meta::cpp2::IndexFields>>>
    getTagIndex(GraphSpaceID space, IndexID index);

    StatusOr<std::shared_ptr<std::pair<EdgeType, meta::cpp2::IndexFields>>>
    getEdgeIndex(GraphSpaceID space, IndexID index);

    StatusOr<std::vector<std::shared_ptr<TagIndexTuple>>>
    getTagIndexes(GraphSpaceID space);

    StatusOr<std::vector<std::shared_ptr<EdgeIndexTuple>>>
    getEdgeIndexes(GraphSpaceID space);

    Status checkTagIndexed(GraphSpaceID space, TagID tagID);

    Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType);

protected:
    folly::RWSpinLock tagIndexLock_;
    folly::RWSpinLock edgeIndexLock_;

    std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<TagIndexTuple>>> tagIndexes_;
    std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<EdgeIndexTuple>>> edgeIndexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // META_ADHOCINDEXMANAGER_H_
