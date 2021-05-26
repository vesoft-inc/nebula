/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_SERVERBASEDSCHEMAMANAGER_H_
#define COMMON_META_SERVERBASEDSCHEMAMANAGER_H_

#include "common/base/Base.h"
#include <folly/RWSpinLock.h>
#include "common/meta/SchemaManager.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace meta {

class ServerBasedSchemaManager : public SchemaManager {
public:
    ServerBasedSchemaManager() = default;
    ~ServerBasedSchemaManager();

    StatusOr<int32_t> getSpaceVidLen(GraphSpaceID space) override;

    StatusOr<cpp2::PropertyType> getSpaceVidType(GraphSpaceID space) override;

    StatusOr<int32_t> getPartsNum(GraphSpaceID space) override;

    // return the newest one if ver less 0
    std::shared_ptr<const NebulaSchemaProvider>
    getTagSchema(GraphSpaceID space, TagID tag, SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) override;

    // return the newest one if ver less 0
    std::shared_ptr<const NebulaSchemaProvider>
    getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) override;

    StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) override;

    StatusOr<std::string> toGraphSpaceName(GraphSpaceID space) override;

    StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    StatusOr<std::string> toTagName(GraphSpaceID space, TagID tagId) override;

    StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) override;

    StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID space) override;

    // get all version of all tags
    StatusOr<TagSchemas> getAllVerTagSchema(GraphSpaceID space) override;

    // get latest version of all tags
    StatusOr<TagSchema> getAllLatestVerTagSchema(GraphSpaceID space) override;

    // get all version of all edges
    StatusOr<EdgeSchemas> getAllVerEdgeSchema(GraphSpaceID space) override;

    // get all latest version of all edges
    StatusOr<EdgeSchema> getAllLatestVerEdgeSchema(GraphSpaceID space) override;

    StatusOr<std::vector<nebula::meta::cpp2::FTClient>> getFTClients() override;

    StatusOr<std::pair<std::string, nebula::meta::cpp2::FTIndex>>
    getFTIndex(GraphSpaceID spaceId, int32_t schemaId) override;

    void init(MetaClient *client);

    static std::unique_ptr<ServerBasedSchemaManager> create(MetaClient *client);

private:
    MetaClient             *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_SERVERBASEDSCHEMAMANAGER_H_
