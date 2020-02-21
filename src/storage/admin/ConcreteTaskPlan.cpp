/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/ConcreteTaskPlan.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

void CompactPlan::run() {
    LOG(INFO) << "messi " << __PRETTY_FUNCTION__;
}

void CompactPlan::stop() {
    LOG(INFO) << "messi " << __PRETTY_FUNCTION__;
}

}  // namespace storage
}  // namespace nebula
