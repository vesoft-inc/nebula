/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    friend class SchemaManager;
public:
    ServerBasedSchemaManager() = default;

    std::shared_ptr<const SchemaProviderIf> getTagSchema(
        GraphSpaceID space, TagID tag, int32_t ver = -1) override;

    std::shared_ptr<const SchemaProviderIf> getTagSchema(
        folly::StringPiece spaceName,
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
    // Returns a negative number when the schema does not exist
    int32_t getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) override;

    int32_t getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                   folly::StringPiece typeName) override;

    GraphSpaceID toGraphSpaceID(folly::StringPiece spaceName) override;

    TagID toTagID(folly::StringPiece tagName) override;

    EdgeType toEdgeType(folly::StringPiece typeName) override;

    void init() override {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SERVERBASEDSCHEMAMANAGER_H_

