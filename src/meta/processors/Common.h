/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_COMMON_H_
#define META_PROCESSORS_COMMON_H_

#include "utils/Types.h"

namespace nebula {
namespace meta {

static const PartitionID kDefaultPartId = 0;
static const GraphSpaceID kDefaultSpaceId = 0;

using BalanceID = int64_t;

enum class BalanceTaskStatus : uint8_t {
    START                   = 0x01,
    CHANGE_LEADER           = 0x02,
    ADD_PART_ON_DST         = 0x03,
    ADD_LEARNER             = 0x04,
    CATCH_UP_DATA           = 0x05,
    MEMBER_CHANGE_ADD       = 0x06,
    MEMBER_CHANGE_REMOVE    = 0x07,
    UPDATE_PART_META        = 0x08,  // After this state, we can't rollback anymore.
    REMOVE_PART_ON_SRC      = 0x09,
    CHECK                   = 0x0A,
    END                     = 0xFF,
};

enum class BalanceTaskResult : uint8_t {
    SUCCEEDED           = 0x01,
    FAILED              = 0x02,
    IN_PROGRESS         = 0x03,
    INVALID             = 0x04,
};

enum class BalanceStatus : uint8_t {
    NOT_START          = 0x01,
    IN_PROGRESS        = 0x02,
    SUCCEEDED          = 0x03,
    /**
     * TODO(heng): Currently, after the plan failed, we will try to resume it
     * when running "balance" again. But in many cases, the plan will be failed
     * forever, it this cases, we should rollback the plan.
     * */
    FAILED             = 0x04,
};

class LockUtils {
public:
    LockUtils() = delete;
#define GENERATE_LOCK(Entry) \
    static folly::SharedMutex& Entry##Lock() { \
        static folly::SharedMutex l; \
        return l; \
    }

GENERATE_LOCK(lastUpdateTime);
GENERATE_LOCK(space);
GENERATE_LOCK(id);
GENERATE_LOCK(tag);
GENERATE_LOCK(edge);
GENERATE_LOCK(tagIndex);
GENERATE_LOCK(edgeIndex);
GENERATE_LOCK(fulltextServices);
GENERATE_LOCK(fulltextIndex);
GENERATE_LOCK(user);
GENERATE_LOCK(config);
GENERATE_LOCK(snapshot);
GENERATE_LOCK(group);
GENERATE_LOCK(zone);
GENERATE_LOCK(listener);
GENERATE_LOCK(session);

#undef GENERATE_LOCK
};


}  // namespace meta
}  // namespace nebula
#endif  // META_PROCESSORS_COMMON_H_

