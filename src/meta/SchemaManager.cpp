/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/SchemaManager.h"

namespace nebula {
namespace meta {

// static
SchemaManager* SchemaManager::instance() {
    return new MemorySchemaManager();
}


}  // namespace meta
}  // namespace nebula

