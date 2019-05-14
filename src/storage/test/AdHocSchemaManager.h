/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADHOCSCHEMAMANAGER_H_
#define META_ADHOCSCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace storage {

class AdHocSchemaManager final : public nebula::meta::SchemaManager {
public:
    AdHocSchemaManager() = default;
    ~AdHocSchemaManager() = default;

    void addTagSchema(GraphSpaceID space,
                      TagID tag,
                      std::shared_ptr<nebula::meta::SchemaProviderIf> schema);

    void addEdgeSchema(GraphSpaceID space,
                       EdgeType edge,
                       std::shared_ptr<nebula::meta::SchemaProviderIf> schema);

    std::shared_ptr<const nebula::meta::SchemaProviderIf> getTagSchema(GraphSpaceID space,
                                                                       TagID tag,
                                                                       SchemaVer ver = -1) override;

    std::shared_ptr<const nebula::meta::SchemaProviderIf> getTagSchema(folly::StringPiece spaceName,
                                                                       folly::StringPiece tagName,
                                                                       SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    SchemaVer getNewestTagSchemaVer(GraphSpaceID space, TagID tag) override;

    SchemaVer getNewestTagSchemaVer(folly::StringPiece spaceName,
                                    folly::StringPiece tagName) override;

    std::shared_ptr<const nebula::meta::SchemaProviderIf>
    getEdgeSchema(GraphSpaceID space,
                  EdgeType edge,
                  SchemaVer ver = -1) override;

    std::shared_ptr<const nebula::meta::SchemaProviderIf>
    getEdgeSchema(folly::StringPiece spaceName,
                  folly::StringPiece typeName,
                  SchemaVer ver = -1) override;

    // Returns a negative number when the schema does not exist
    SchemaVer getNewestEdgeSchemaVer(GraphSpaceID space, EdgeType edge) override;

    SchemaVer getNewestEdgeSchemaVer(folly::StringPiece spaceName,
                                     folly::StringPiece typeName) override;

    virtual GraphSpaceID toGraphSpaceID(folly::StringPiece spaceName);

    TagID toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    EdgeType toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    void init(nebula::meta::MetaClient *client = nullptr) override { UNUSED(client); }

protected:
    folly::RWSpinLock tagLock_;
    std::unordered_map<std::pair<GraphSpaceID, TagID>,
    // version -> schema
    std::map<SchemaVer, std::shared_ptr<const nebula::meta::SchemaProviderIf>>>
            tagSchemas_;

    folly::RWSpinLock edgeLock_;
    std::unordered_map<std::pair<GraphSpaceID, EdgeType>,
    // version -> schema
    std::map<SchemaVer, std::shared_ptr<const nebula::meta::SchemaProviderIf>>>
            edgeSchemas_;
};

}  // namespace storage
}  // namespace nebula
#endif  // META_ADHOCSCHEMAMANAGER_H_


