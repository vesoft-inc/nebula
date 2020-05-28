/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_WALLCLOCK_H_
#define COMMON_TIME_WALLCLOCK_H_

#include "common/base/Base.h"
#include <time.h>

namespace nebula {
namespace time {

/**
 * The class provides several utility methods to allow fast access the time
 *
 * The *fast* versions are way faster than the *slow* versions, but not as
 * precise as the *slow* versions. Choose wisely
 *
 */
class WallClock final {
public:
    WallClock() = delete;

    static int64_t slowNowInSec();
    static int64_t fastNowInSec();

    static int64_t slowNowInMilliSec();
    static int64_t fastNowInMilliSec();

    static int64_t slowNowInMicroSec();
    static int64_t fastNowInMicroSec();
};

}  // namespace time
}  // namespace nebula
#endif  // COMMON_TIME_WALLCLOCK_H_
