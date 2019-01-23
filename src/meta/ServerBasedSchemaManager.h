/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SERVERBASEDSCHEMAMANAGER_H_
#define META_SERVERBASEDSCHEMAMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include "meta/SchemaManager.h"

namespace nebula {
namespace meta {

class ServerBasedSchemaManager : public SchemaManager {
    friend class SchemaManager;
public:
    static GraphSpaceID toGraphSpaceID(const folly::StringPiece spaceName);
    static TagID toTagID(const folly::StringPiece tagName);
    static EdgeType toEdgeType(const folly::StringPiece typeName);

private:
    ServerBasedSchemaManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SERVERBASEDSCHEMAMANAGER_H_

