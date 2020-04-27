/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SCHEMAMANAGER_H_
#define META_SCHEMAMANAGER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class MetaClient;

class SchemaManager {
public:
    virtual ~SchemaManager() = default;

    static std::unique_ptr<SchemaManager> create(MetaClient *client);

    virtual std::shared_ptr<const SchemaProviderIf>
    getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver = -1) = 0;

    // Returns a negative number when the schema does not exist
    virtual StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) = 0;

    virtual std::shared_ptr<const SchemaProviderIf>
    getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver = -1) = 0;

    // Returns a negative number when the schema does not exist
    virtual StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) = 0;

    virtual StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) = 0;

    virtual StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) = 0;

    virtual StatusOr<std::string> toTagName(GraphSpaceID space, TagID tagId) = 0;

    virtual StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) = 0;

    virtual StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) = 0;

    virtual StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID space) = 0;

protected:
    SchemaManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_
