/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCHEMAMANAGER_H_
#define META_SCHEMAMANAGER_H_

#include "base/Base.h"
#include "dataman/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class SchemaManager {
public:
    static SchemaManager* instance();

    virtual SchemaProviderIf* edgeSchema(GraphSpaceID spaceId, EdgeType edgeType) = 0;

    virtual SchemaProviderIf* tagSchema(GraphSpaceID spaceId, TagID tagId) = 0;

    virtual int32_t version() = 0;

    virtual ~SchemaManager() = default;

protected:
    SchemaManager() = default;
};

class MemorySchemaManager : public SchemaManager {
public:
    using EdgeSchema
        = std::unordered_map<GraphSpaceID, std::unordered_map<EdgeType, SchemaProviderIf*>>;

    using TagSchema
        = std::unordered_map<GraphSpaceID, std::unordered_map<TagID, SchemaProviderIf*>>;

    SchemaProviderIf* edgeSchema(GraphSpaceID spaceId, EdgeType edgeType) override {
        auto it = edgeSchema_.find(spaceId);
        if (it != edgeSchema_.end()) {
            auto edgeIt = it->second.find(edgeType);
            if (edgeIt != it->second.end()) {
                return edgeIt->second;
            }
        }
        return nullptr;
    }

    SchemaProviderIf* tagSchema(GraphSpaceID spaceId, TagID tagId) override {
        auto it = tagSchema_.find(spaceId);
        if (it != tagSchema_.end()) {
            auto tagIt = it->second.find(tagId);
            if (tagIt != it->second.end()) {
                return tagIt->second;
            }
        }
        return nullptr;
    }

    int32_t version() {
        return 0;
    }

    EdgeSchema& edgeSchema() {
        return edgeSchema_;
    }

    TagSchema& tagSchema() {
        return tagSchema_;
    }

private:
    EdgeSchema edgeSchema_;

    TagSchema tagSchema_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_


