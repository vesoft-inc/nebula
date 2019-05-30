/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/IndexManager.h"

namespace nebula {
namespace meta {

std::unique_ptr<IndexManager> IndexManager::create() {
    auto manager = std::unique_ptr<IndexManager>(new ServerBasedIndexManager());
    return manager;
}

}  // namespace meta
}  // namespace nebula

