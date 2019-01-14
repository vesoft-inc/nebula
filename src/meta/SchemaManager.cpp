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


MemorySchemaManager::~MemorySchemaManager() {
    for (auto &space : edgeSchema_) {
        for (auto &schema : space.second) {
            delete schema.second;
        }
    }
    for (auto &space : tagSchema_) {
        for (auto &schema : space.second) {
            delete schema.second;
        }
    }
}


}  // namespace meta
}  // namespace nebula

