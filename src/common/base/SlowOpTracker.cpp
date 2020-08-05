/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "time/WallClock.h"
#include "base/SlowOpTracker.h"

DEFINE_int64(slow_op_threshhold_ms, 50, "default threshhold for slow operation");
