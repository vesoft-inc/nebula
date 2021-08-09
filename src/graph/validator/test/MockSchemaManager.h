/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef VALIDATOR_MOCKSCHEMAMANAGER_H_
#define VALIDATOR_MOCKSCHEMAMANAGER_H_

#include "common/meta/SchemaManager.h"
#include "common/meta/NebulaSchemaProvider.h"

namespace nebula {
namespace graph {

class MockSchemaManager final : public nebula::meta::SchemaManager {
public:
    MockSchemaManager() = default;
    ~MockSchemaManager() = default;

    static std::unique_ptr<MockSchemaManager> makeUnique() {
        auto instance = std::make_unique<MockSchemaManager>();
        instance->init();
        return instance;
    }

    void init();

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getTagSchema(GraphSpaceID space, TagID tag, SchemaVer version = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) override {
        UNUSED(space);
        UNUSED(tag);
        return -1;
    }

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getEdgeSchema(GraphSpaceID space, EdgeType edge, SchemaVer version = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) override {
        UNUSED(space);
        UNUSED(edge);
        return -1;
    }

    StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) override;

    StatusOr<std::string> toGraphSpaceName(GraphSpaceID space) override;

    StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    StatusOr<std::string> toTagName(GraphSpaceID space, TagID tagId) override;

    StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) override;

    StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID) override;

    StatusOr<int32_t> getSpaceVidLen(GraphSpaceID space) override {
        UNUSED(space);
        return 8;
    }

    // Returns all version of all tags
    StatusOr<meta::TagSchemas> getAllVerTagSchema(GraphSpaceID space) override {
        meta::TagSchemas allVerTagSchemas;
        const auto& tagSchemas = tagSchemas_[space];
        for (const auto &tagSchema : tagSchemas) {
            allVerTagSchemas.emplace(tagSchema.first,
                                     std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>
                                        {tagSchema.second});
        }
        return allVerTagSchemas;
    }

    // Returns all latest version of schesmas of all tags in the given space
    StatusOr<meta::TagSchema> getAllLatestVerTagSchema(GraphSpaceID space) override {
        meta::TagSchema allLatestVerTagSchemas;
        const auto& tagSchemas = tagSchemas_[space];
        for (const auto &tagSchema : tagSchemas) {
            allLatestVerTagSchemas.emplace(tagSchema.first, tagSchema.second);
        }
        return allLatestVerTagSchemas;
    }

    // Returns all version of all edges
    StatusOr<meta::EdgeSchemas> getAllVerEdgeSchema(GraphSpaceID space) override {
        meta::EdgeSchemas allVerEdgeSchemas;
        const auto& edgeSchemas = edgeSchemas_[space];
        for (const auto &edgeSchema : edgeSchemas) {
            allVerEdgeSchemas.emplace(edgeSchema.first,
                                     std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>
                                        {edgeSchema.second});
        }
        return allVerEdgeSchemas;
    }

    // Returns all latest version of  edges schema
    StatusOr<meta::EdgeSchema> getAllLatestVerEdgeSchema(GraphSpaceID space) override {
        meta::EdgeSchema allLatestVerEdgeSchemas;
        const auto& edgeSchemas = edgeSchemas_[space];
        for (const auto &edgeSchema : edgeSchemas) {
            allLatestVerEdgeSchemas.emplace(edgeSchema.first, edgeSchema.second);
        }
        return allLatestVerEdgeSchemas;
    }

    StatusOr<std::vector<nebula::meta::cpp2::FTClient> > getFTClients() override;

    StatusOr<int32_t> getPartsNum(GraphSpaceID) override {
        LOG(FATAL) << "Unimplemented.";
    }

    StatusOr<std::pair<std::string, nebula::meta::cpp2::FTIndex>>
    getFTIndex(GraphSpaceID, int32_t) override {
        LOG(FATAL) << "Unimplemented";
        return Status::Error("Unimplemented");
    }

private:
    std::unordered_map<std::string, GraphSpaceID>        spaceNameIds_;
    std::unordered_map<std::string, TagID>               tagNameIds_;
    std::unordered_map<TagID, std::string>               tagIdNames_;
    std::unordered_map<std::string, EdgeType>            edgeNameIds_;
    std::unordered_map<EdgeType, std::string>            edgeIdNames_;
    using Tags = std::unordered_map<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>;
    std::unordered_map<GraphSpaceID, Tags>               tagSchemas_;
    using Edges = std::unordered_map<EdgeType, std::shared_ptr<const meta::NebulaSchemaProvider>>;
    std::unordered_map<GraphSpaceID, Edges>              edgeSchemas_;
};

}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_MOCKSCHEMAMANAGER_H_
