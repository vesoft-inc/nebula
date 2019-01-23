/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_FILEBASEDSCHEMAMANAGER_H_
#define META_FILEBASEDSCHEMAMANAGER_H_

#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "base/Configuration.h"

namespace nebula {
namespace meta {

class FileBasedSchemaManager final : public SchemaManager {
    friend class SchemaManager;
private:
    static void init();

    static void readOneGraphSpace(GraphSpaceID space, const Configuration& conf);
    static std::shared_ptr<const SchemaProviderIf> readSchema(const folly::dynamic& fields);

    static GraphSpaceID toGraphSpaceID(const folly::StringPiece spaceName);
    static TagID toTagID(const folly::StringPiece tagName);
    static EdgeType toEdgeType(const folly::StringPiece typeName);

    FileBasedSchemaManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_FILEBASEDSCHEMAMANAGER_H_

