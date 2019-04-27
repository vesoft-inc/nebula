/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCHEMAMANAGER_H_
#define META_SCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

class SchemaManager {
public:
    static std::unique_ptr<SchemaManager> create();

    virtual std::shared_ptr<const SchemaProviderIf> getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver = -1) = 0;

    virtual std::shared_ptr<const SchemaProviderIf> getTagSchema(
        folly::StringPiece spaceName,
        folly::StringPiece tagName,
        int32_t ver = -1) = 0;

    virtual bool removeTagSchema(GraphSpaceID space,
                                 TagID tag,
                                 int32_t ver = -1) = 0;

    virtual bool removeTagSchema(folly::StringPiece spaceName,
                                 folly::StringPiece tagName,
                                 int32_t ver = -1) = 0;

    // Returns a negative number when the schema does not exist
    virtual int32_t getNewestTagSchemaVer(GraphSpaceID space, TagID tag) = 0;

    virtual int32_t getNewestTagSchemaVer(folly::StringPiece spaceName,
                                          folly::StringPiece tagName) = 0;

    virtual std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        GraphSpaceID space, EdgeType edge, int32_t ver = -1) = 0;

    virtual std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        folly::StringPiece spaceName,
        folly::StringPiece typeName,
        int32_t ver = -1) = 0;

    virtual bool removeEdgeSchema(GraphSpaceID space,
                                  EdgeType edge,
                                  int32_t ver = -1) = 0;

    virtual bool removeEdgeSchema(folly::StringPiece spaceName,
                                  folly::StringPiece edgeName,
                                  int32_t ver = -1) = 0;

    // Returns a negative number when the schema does not exist
    virtual int32_t getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) = 0;

    virtual int32_t getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                           folly::StringPiece typeName) = 0;

    virtual GraphSpaceID toGraphSpaceID(folly::StringPiece spaceName) = 0;

    virtual TagID toTagID(folly::StringPiece tagName) = 0;

    virtual EdgeType toEdgeType(folly::StringPiece typeName) = 0;

    virtual void init() = 0;

protected:
    SchemaManager() = default;
};

class AdHocSchemaManager : public SchemaManager {
public:
    AdHocSchemaManager() = default;

    ~AdHocSchemaManager() = default;

    void addTagSchema(GraphSpaceID space,
                      TagID tag,
                      std::shared_ptr<SchemaProviderIf> schema);

    void addEdgeSchema(GraphSpaceID space,
                       EdgeType edge,
                       std::shared_ptr<SchemaProviderIf> schema);

    std::shared_ptr<const SchemaProviderIf> getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver = -1) override;

    std::shared_ptr<const SchemaProviderIf> getTagSchema(
        folly::StringPiece spaceName,
        folly::StringPiece tagName,
        int32_t ver = -1) override;

    bool removeTagSchema(GraphSpaceID space,
                         TagID tag,
                         int32_t ver = -1) override;

    bool removeTagSchema(folly::StringPiece spaceName,
                         folly::StringPiece tagName,
                         int32_t ver = -1) override;

    // Returns a negative number when the schema does not exist
    int32_t getNewestTagSchemaVer(GraphSpaceID space, TagID tag) override;

    int32_t getNewestTagSchemaVer(folly::StringPiece spaceName,
                                  folly::StringPiece tagName) override;

    std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        GraphSpaceID space, EdgeType edge, int32_t ver = -1) override;

    std::shared_ptr<const SchemaProviderIf> getEdgeSchema(
        folly::StringPiece spaceName,
        folly::StringPiece typeName,
        int32_t ver = -1) override;

    bool removeEdgeSchema(GraphSpaceID space,
                          EdgeType edge,
                          int32_t ver = -1) override;

    bool removeEdgeSchema(folly::StringPiece spaceName,
                          folly::StringPiece edgeName,
                          int32_t ver = -1) override;

    // Returns a negative number when the schema does not exist
    int32_t getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) override;

    int32_t getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                   folly::StringPiece typeName) override;

    virtual GraphSpaceID toGraphSpaceID(folly::StringPiece spaceName);

    TagID toTagID(folly::StringPiece tagName) override;

    EdgeType toEdgeType(folly::StringPiece typeName) override;

    void init() override {}

protected:
    folly::RWSpinLock tagLock_;
    std::unordered_map<std::pair<GraphSpaceID, TagID>,
                       // version -> schema
                       std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>>
        tagSchemas_;

    folly::RWSpinLock edgeLock_;
    std::unordered_map<std::pair<GraphSpaceID, EdgeType>,
                       // version -> schema
                       std::map<int32_t, std::shared_ptr<const SchemaProviderIf>>>
        edgeSchemas_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_

