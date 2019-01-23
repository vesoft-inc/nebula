/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADHOCSCHEMAMANAGER_H_
#define META_ADHOCSCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaManager.h"

namespace nebula {
namespace meta {

//
// This class is ONLY meant for test purpose. Please DO NOT use it
// in the production environment!!!!
//
class AdHocSchemaManager : public SchemaManager {
    friend class SchemaManager;
public:
    static void init() {}

    static void addTagSchema(GraphSpaceID space,
                             TagID tag,
                             std::shared_ptr<SchemaProviderIf> schema);
    static void addEdgeSchema(GraphSpaceID space,
                              EdgeType edge,
                              std::shared_ptr<SchemaProviderIf> schema);

    static GraphSpaceID toGraphSpaceID(const folly::StringPiece spaceName);
    static TagID toTagID(const folly::StringPiece tagName);
    static EdgeType toEdgeType(const folly::StringPiece typeName);

private:
    AdHocSchemaManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADHOCSCHEMAMANAGER_H_

