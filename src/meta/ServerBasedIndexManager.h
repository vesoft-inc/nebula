/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SERVERBASEDINDEXMANAGER_H_
#define META_SERVERBASEDINDEXMANAGER_H_

#include "meta/IndexManager.h"

namespace nebula {
namespace meta {

class ServerBasedIndexManager : public IndexManager {
public:
    ServerBasedIndexManager() = default;
    ~ServerBasedIndexManager();

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getTagIndex(GraphSpaceID space, IndexID index) override;

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getEdgeIndex(GraphSpaceID space, IndexID index) override;

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getTagIndexes(GraphSpaceID space) override;

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getEdgeIndexes(GraphSpaceID space) override;

    StatusOr<IndexID>
    toTagIndexID(GraphSpaceID space, std::string tagName) override;

    StatusOr<IndexID>
    toEdgeIndexID(GraphSpaceID space, std::string edgeName) override;

    Status checkTagIndexed(GraphSpaceID space, TagID tagID) override;

    Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) override;

    void init(MetaClient *client) override;

private:
    MetaClient             *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SERVERBASEDINDEXMANAGER_H_
