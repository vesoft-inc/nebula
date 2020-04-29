/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_ADHOCSCHEMAMANAGER_H_
#define MOCK_ADHOCSCHEMAMANAGER_H_

#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace mock {

class AdHocSchemaManager final : public nebula::meta::SchemaManager {
public:
    AdHocSchemaManager() = default;
    ~AdHocSchemaManager() = default;

    void addTagSchema(GraphSpaceID space,
                      TagID tag,
                      std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema);

    void addEdgeSchema(GraphSpaceID space,
                       EdgeType edge,
                       std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema);

    void removeTagSchema(GraphSpaceID space, TagID tag);

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getTagSchema(GraphSpaceID space,
                 TagID tag,
                 SchemaVer version = -1) override;


    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) override;

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getEdgeSchema(GraphSpaceID space,
                  EdgeType edge,
                  SchemaVer version = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) override;

    StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) override;

    StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    StatusOr<std::string> toTagName(GraphSpaceID, TagID) override {
        LOG(FATAL) << "Unimplemented";
        return Status::Error("Unimplemented");
    }

    StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) override;

    StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID) override {
        LOG(FATAL) << "Unimplemented";
        return Status::Error("Unimplemented");
    }

    StatusOr<int32_t> getSpaceVidLen(GraphSpaceID space) override;

    std::vector<std::pair<TagID, std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>>
    listLatestTagSchema(GraphSpaceID space) override;

    std::vector<std::pair<EdgeType, std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>>
    listLatestEdgeSchema(GraphSpaceID space) override;

protected:
    folly::RWSpinLock tagLock_;
    std::unordered_map<std::pair<GraphSpaceID, TagID>,
                       // version -> schema
                       std::map<SchemaVer, std::shared_ptr<const meta::NebulaSchemaProvider>>>
        tagSchemas_;

    folly::RWSpinLock edgeLock_;
    std::unordered_map<std::pair<GraphSpaceID, EdgeType>,
                       // version -> schema
                       std::map<SchemaVer, std::shared_ptr<const meta::NebulaSchemaProvider>>>
        edgeSchemas_;

    folly::RWSpinLock spaceLock_;
    std::set<GraphSpaceID> spaces_;
    // Key: spaceId + tagName,  Val: tagId
    std::unordered_map<std::string, TagID> tagNameToId_;
};

}  // namespace mock
}  // namespace nebula
#endif  // MOCK_ADHOCSCHEMAMANAGER_H_
