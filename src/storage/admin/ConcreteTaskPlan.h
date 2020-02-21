/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKCONCRETEPLAN_H_
#define STORAGE_ADMIN_ADMINTASKCONCRETEPLAN_H_

#include "storage/admin/AdminTaskPlan.h"
#include "common/base/ThriftTypes.h"

namespace nebula {
namespace storage {

class CompactPlan : AdminTaskPlan {
public:
    explicit CompactPlan(GraphSpaceID spaceId);
    void run() override;
    void stop() override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKCONCRETEPLAN_H_
