/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKPLAN_H_
#define STORAGE_ADMIN_ADMINTASKPLAN_H_

#include "common/base/ThriftTypes.h"

namespace nebula {
namespace storage {

class AdminTaskPlan {
public:
    explicit AdminTaskPlan(GraphSpaceID spaceId) : spaceId_(spaceId) {}
    virtual void run() = 0;
    virtual void stop() = 0;
    virtual ~AdminTaskPlan() {}

private:
    GraphSpaceID spaceId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKPLAN_H_
