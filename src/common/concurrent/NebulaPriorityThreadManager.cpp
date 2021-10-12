/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/concurrent/NebulaPriorityThreadManager.h"

using apache::thrift::concurrency::PriorityThreadManager;

namespace nebula {
namespace concurrency {

NebulaPriorityThreadManager::NebulaPriorityThreadManager(size_t normalThreadsCount,
                                                         bool enableTaskStats) {
  proxy_ = PriorityThreadManager::newPriorityThreadManager(normalThreadsCount, enableTaskStats);
}

}  // namespace concurrency
}  // namespace nebula
