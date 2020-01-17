/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/IndexManager.h"
#include "meta/ServerBasedIndexManager.h"

namespace nebula {
namespace meta {

std::unique_ptr<IndexManager> IndexManager::create() {
    return std::make_unique<ServerBasedIndexManager>();
}

}   // namespace meta
}   // namespace nebula

