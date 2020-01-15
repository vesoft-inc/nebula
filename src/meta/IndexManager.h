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
    virtual ~IndexManager() = default;

    static std::unique_ptr<IndexManager> create();

    virtual void init(MetaClient *client) = 0;

    virtual StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getTagIndex(GraphSpaceID space, IndexID index) = 0;

    virtual StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getEdgeIndex(GraphSpaceID space, IndexID index) = 0;

    virtual StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getTagIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getEdgeIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<IndexID>
    toTagIndexID(GraphSpaceID space, std::string tagName) = 0;

    virtual StatusOr<IndexID>
    toEdgeIndexID(GraphSpaceID space, std::string edgeName) = 0;

    virtual Status checkTagIndexed(GraphSpaceID space, TagID tagID) = 0;

    virtual Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) = 0;

protected:
    IndexManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_INDEXMANAGER_H_
