/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/AdHocSchemaManager.h"

namespace nebula {
namespace meta {

// static
void AdHocSchemaManager::addTagSchema(GraphSpaceID space,
                                      TagID tag,
                                      std::shared_ptr<SchemaProviderIf> schema) {
    folly::RWSpinLock::WriteHolder wh(SchemaManager::tagLock_);
    // Only version 0
    tagSchemas_[std::make_pair(space, tag)][0] = schema;
}


// static
void AdHocSchemaManager::addEdgeSchema(GraphSpaceID space,
                                       EdgeType edge,
                                       std::shared_ptr<SchemaProviderIf> schema) {
    folly::RWSpinLock::WriteHolder wh(SchemaManager::edgeLock_);
    // Only version 0
    edgeSchemas_[std::make_pair(space, edge)][0] = schema;
}


// static
GraphSpaceID AdHocSchemaManager::toGraphSpaceID(const folly::StringPiece spaceName) {
    return folly::hash::fnv32_buf(spaceName.start(), spaceName.size());
}


// static
TagID AdHocSchemaManager::toTagID(const folly::StringPiece tagName) {
    return folly::hash::fnv32_buf(tagName.start(), tagName.size());
}


// static
EdgeType AdHocSchemaManager::toEdgeType(const folly::StringPiece typeName) {
    return folly::hash::fnv32_buf(typeName.start(), typeName.size());
}

}  // namespace meta
}  // namespace nebula

