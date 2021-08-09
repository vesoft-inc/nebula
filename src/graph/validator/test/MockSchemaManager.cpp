/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "validator/test/MockSchemaManager.h"
#include <memory>

namespace nebula {
namespace graph {

// space: test_space
// tag:
//     person(name string, age int8)
//     book(name string)
//     room(number int8)
// edge:
//     like(start timestamp, end timestamp, likeness int64)
void MockSchemaManager::init() {
    spaceNameIds_.emplace("test_space", 1);
    tagNameIds_.emplace("person", 2);
    tagIdNames_.emplace(2, "person");
    edgeNameIds_.emplace("like", 3);
    edgeIdNames_.emplace(3, "like");
    edgeNameIds_.emplace("serve", 4);
    edgeIdNames_.emplace(4, "serve");
    tagNameIds_.emplace("book", 5);
    tagIdNames_.emplace(5, "book");
    tagNameIds_.emplace("room", 6);
    tagIdNames_.emplace(6, "room");

    Tags tagSchemas;
    // person {name : string, age : int8}
    std::shared_ptr<meta::NebulaSchemaProvider> personSchema(new meta::NebulaSchemaProvider(0));
    personSchema->addField("name", meta::cpp2::PropertyType::STRING);
    personSchema->addField("age", meta::cpp2::PropertyType::INT8);
    tagSchemas.emplace(2, personSchema);
    // book {name : string}
    std::shared_ptr<meta::NebulaSchemaProvider> bookSchema(new meta::NebulaSchemaProvider(0));
    bookSchema->addField("name", meta::cpp2::PropertyType::STRING);
    tagSchemas.emplace(5, bookSchema);
    // room {number : int8}
    std::shared_ptr<meta::NebulaSchemaProvider> roomSchema(new meta::NebulaSchemaProvider(0));
    roomSchema->addField("number", meta::cpp2::PropertyType::INT8);
    tagSchemas.emplace(5, roomSchema);

    tagSchemas_.emplace(1, std::move(tagSchemas));

    Edges edgeSchemas;
    // like {start : timestamp, end : datetime}
    std::shared_ptr<meta::NebulaSchemaProvider> likeSchema(new meta::NebulaSchemaProvider(0));
    likeSchema->addField("start", meta::cpp2::PropertyType::TIMESTAMP);
    likeSchema->addField("end", meta::cpp2::PropertyType::TIMESTAMP);
    likeSchema->addField("likeness", meta::cpp2::PropertyType::INT64);
    edgeSchemas.emplace(3, likeSchema);

    std::shared_ptr<meta::NebulaSchemaProvider> serveSchema(new meta::NebulaSchemaProvider(0));
    serveSchema->addField("start", meta::cpp2::PropertyType::TIMESTAMP);
    serveSchema->addField("end", meta::cpp2::PropertyType::DATETIME);
    edgeSchemas.emplace(4, serveSchema);

    edgeSchemas_.emplace(1, std::move(edgeSchemas));
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
MockSchemaManager::getTagSchema(GraphSpaceID space,
                                 TagID tag,
                                 SchemaVer) {
    auto spaceIt = tagSchemas_.find(space);
    if (spaceIt == tagSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto tagIt = spaceIt->second.find(tag);
    if (tagIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    return tagIt->second;
}

std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
MockSchemaManager::getEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  SchemaVer) {
    auto spaceIt = edgeSchemas_.find(space);
    if (spaceIt == edgeSchemas_.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }
    auto edgeIt = spaceIt->second.find(edge);
    if (edgeIt == spaceIt->second.end()) {
        // Not found
        return std::shared_ptr<const nebula::meta::NebulaSchemaProvider>();
    }

    return edgeIt->second;
}

StatusOr<GraphSpaceID> MockSchemaManager::toGraphSpaceID(folly::StringPiece spaceName) {
    auto findIt = spaceNameIds_.find(spaceName.str());
    if (findIt != spaceNameIds_.end()) {
        return findIt->second;
    }
    return Status::Error("Space `%s' not found", spaceName.str().c_str());
}

StatusOr<std::string> MockSchemaManager::toGraphSpaceName(GraphSpaceID space) {
    for (const auto& s : spaceNameIds_) {
        if (s.second == space) {
            return s.first;
        }
    }
    return Status::Error("Space `%d' not found", space);
}

StatusOr<TagID> MockSchemaManager::toTagID(GraphSpaceID space, const folly::StringPiece tagName) {
    auto findIt = tagSchemas_.find(space);
    if (findIt == tagSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto tagFindIt = tagNameIds_.find(tagName.str());
    if (tagFindIt != tagNameIds_.end()) {
        return tagFindIt->second;
    }
    return Status::Error("TagName `%s' not found", tagName.str().c_str());
}

StatusOr<std::string> MockSchemaManager::toTagName(GraphSpaceID space, TagID tagId) {
    auto findIt = tagSchemas_.find(space);
    if (findIt == tagSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto tagFindIt = tagIdNames_.find(tagId);
    if (tagFindIt != tagIdNames_.end()) {
        return tagFindIt->second;
    }
    return Status::Error("TagID `%d' not found", tagId);
}

StatusOr<EdgeType> MockSchemaManager::toEdgeType(GraphSpaceID space, folly::StringPiece typeName) {
    auto findIt = edgeSchemas_.find(space);
    if (findIt == edgeSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto edgeFindIt = edgeNameIds_.find(typeName.str());
    if (edgeFindIt != edgeNameIds_.end()) {
        return edgeFindIt->second;
    }
    return Status::Error("EdgeName `%s' not found", typeName.str().c_str());
}

StatusOr<std::string> MockSchemaManager::toEdgeName(GraphSpaceID space, EdgeType edgeType) {
    auto findIt = edgeSchemas_.find(space);
    if (findIt == edgeSchemas_.end()) {
        return Status::Error("Space `%d' not found", space);
    }
    auto edgeFindIt = edgeIdNames_.find(edgeType);
    if (edgeFindIt != edgeIdNames_.end()) {
        return edgeFindIt->second;
    }
    return Status::Error("EdgeType `%d' not found", edgeType);
}

StatusOr<std::vector<std::string>> MockSchemaManager::getAllEdge(GraphSpaceID) {
    std::vector<std::string> edgeNames;
    for (const auto &item : edgeNameIds_) {
        edgeNames.emplace_back(item.first);
    }
    return edgeNames;
}

StatusOr<std::vector<nebula::meta::cpp2::FTClient>> MockSchemaManager::getFTClients() {
    return Status::Error("Not implemented");
}

}  // namespace graph
}  // namespace nebula
