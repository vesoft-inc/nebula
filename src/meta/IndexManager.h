/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INDEXMANAGER_H_
#define META_INDEXMANAGER_H_

#include "base/Base.h"
#include "meta/client/MetaClient.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class IndexManager {
public:
    IndexManager() = default;

    ~IndexManager() = default;

    static std::unique_ptr<IndexManager> create();

    void init(MetaClient *client);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getTagIndex(GraphSpaceID space, IndexID index);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getEdgeIndex(GraphSpaceID space, IndexID index);

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getTagIndexes(GraphSpaceID space);

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getEdgeIndexes(GraphSpaceID space);

    StatusOr<IndexID>
    toTagIndexID(GraphSpaceID space, std::string tagName);

    StatusOr<IndexID>
    toEdgeIndexID(GraphSpaceID space, std::string edgeName);

    Status checkTagIndexed(GraphSpaceID space, TagID tagID);

    Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType);

private:
    MetaClient             *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_INDEXMANAGER_H_
