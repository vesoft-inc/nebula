/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_COMMON_H_
#define META_PROCESSORS_COMMON_H_

#include "base/Base.h"

namespace nebula {
namespace meta {

static const PartitionID kDefaultPartId = 0;
static const GraphSpaceID kDefaultSpaceId = 0;

using BalanceID = int64_t;

class LockUtils {
public:
    LockUtils() = delete;
#define GENERATE_LOCK(Entry) \
    static folly::SharedMutex& Entry##Lock() { \
        static folly::SharedMutex l; \
        return l; \
    }

GENERATE_LOCK(space);
GENERATE_LOCK(id);
GENERATE_LOCK(tag);
GENERATE_LOCK(edge);
GENERATE_LOCK(user);
GENERATE_LOCK(config);
GENERATE_LOCK(snapshot);

#undef GENERATE_LOCK
};


}  // namespace meta
}  // namespace nebula
#endif  // META_PROCESSORS_COMMON_H_

