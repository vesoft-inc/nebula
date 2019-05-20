/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INDEXMANAGER_H
#define META_INDEXMANAGER_H

#include "base/Base.h"

namespace nebula {
namespace meta {

class IndexManager {
public:
    virtual ~IndexManager() = default;

    static std::unique_ptr<IndexManager> create();

    virtual void init(MetaClient *client = nullptr) = 0;

    virtual std::shared_ptr<const IndexProviderIf> getTagIndex(GraphSpaceID space,
                                                               TagIndexID indedID) = 0;

    virtual std::shared_ptr<const IndexProviderIf> getTagIndex(folly::StringPiece spaceName,
                                                               folly::StringPiece indexName) = 0;

    virtual std::shared_ptr<const IndexProviderIf> getEdgeIndex(GraphSpaceID space,
                                                                EdgeIndexID indedID) = 0;

    virtual std::shared_ptr<const IndexProviderIf> getEdgeIndex(folly::StringPiece spaceName,
                                                                folly::StringPiece indexName) = 0;

    virtual TagIndexID toTagIndexID(GraphSpaceID space, folly::StringPiece tagIndexName) = 0;

    virtual EdgeIndexID toEdgeIndexID(GraphSpaceID space, folly::StringPiece edgeIndexName) = 0;

};

}  // namespace meta
}  // namespace nebula

#endif // META_INDEXMANAGER_H
