/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_INDEXMANAGER_H_
#define COMMON_META_INDEXMANAGER_H_

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {
using IndexItem = cpp2::IndexItem;

class IndexManager {
public:
    virtual ~IndexManager() = default;

    virtual StatusOr<std::shared_ptr<IndexItem>>
    getTagIndex(GraphSpaceID space, IndexID index) = 0;

    virtual StatusOr<std::shared_ptr<IndexItem>>
    getEdgeIndex(GraphSpaceID space, IndexID index) = 0;

    virtual StatusOr<std::vector<std::shared_ptr<IndexItem>>>
    getTagIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<std::vector<std::shared_ptr<IndexItem>>>
    getEdgeIndexes(GraphSpaceID space) = 0;

    virtual StatusOr<IndexID>
    toTagIndexID(GraphSpaceID space, std::string tagName) = 0;

    virtual StatusOr<IndexID>
    toEdgeIndexID(GraphSpaceID space, std::string edgeName) = 0;

    virtual Status checkTagIndexed(GraphSpaceID space, IndexID index) = 0;

    virtual Status checkEdgeIndexed(GraphSpaceID space, IndexID index) = 0;

protected:
    IndexManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_INDEXMANAGER_H_
