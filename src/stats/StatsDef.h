/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STATS_STATSDEF_H_
#define STATS_STATSDEF_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

DECLARE_int32(slow_query_threshold_us);


namespace nebula {

extern stats::CounterId kNumQueries;
extern stats::CounterId kNumSlowQueries;
extern stats::CounterId kNumQueryErrors;
extern stats::CounterId kQueryLatencyUs;
extern stats::CounterId kSlowQueryLatencyUs;

void initCounters();

}  // namespace nebula
#endif  // STATS_STATSDEF_H_
