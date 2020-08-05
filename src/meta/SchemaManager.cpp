/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace meta {

std::unique_ptr<SchemaManager> SchemaManager::create() {
    return std::make_unique<ServerBasedSchemaManager>();
}

}   // namespace meta
}   // namespace nebula
