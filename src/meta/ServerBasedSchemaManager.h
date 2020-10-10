/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SERVERBASEDSCHEMAMANAGER_H_
#define META_SERVERBASEDSCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/client/MetaClient.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace meta {

class ServerBasedSchemaManager : public SchemaManager {
public:
    ServerBasedSchemaManager() = default;
    ~ServerBasedSchemaManager();

    // return the newest one if ver less 0
    std::shared_ptr<const SchemaProviderIf>
    getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) override;

    // return the newest one if ver less 0
    std::shared_ptr<const SchemaProviderIf>
    getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) override;

    StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) override;

    StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    StatusOr<std::string> toTagName(GraphSpaceID space, TagID tagId) override;

    StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) override;

    StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID space) override;

    StatusOr<std::vector<std::string>> getAllTag(GraphSpaceID space) override;

    void init(MetaClient *client) override;

private:
    MetaClient             *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SERVERBASEDSCHEMAMANAGER_H_
