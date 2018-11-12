/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/SchemaManager.h"

namespace nebula {
namespace meta {


SchemaManager* SchemaManager::instance_;
static std::once_flag initSchemaFlag;
// static
SchemaManager* SchemaManager::instance() {
    std::call_once(initSchemaFlag, [](){
        instance_ = new MemorySchemaManager();
    });
    return instance_;
}


}  // namespace meta
}  // namespace nebula

