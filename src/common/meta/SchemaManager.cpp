/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace meta {

StatusOr<std::pair<bool, int32_t>>
SchemaManager::getSchemaIDByName(GraphSpaceID space, folly::StringPiece schemaName) {
    auto ret = toEdgeType(space, schemaName);
    if (ret.ok()) {
        return std::make_pair(true, ret.value());
    } else {
        ret = toTagID(space, schemaName);
        if (ret.ok()) {
            return std::make_pair(false, ret.value());
        }
    }
    return Status::Error("Schema not exist: %s", schemaName.str().c_str());
}

}   // namespace meta
}   // namespace nebula
