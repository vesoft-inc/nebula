/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_STATS_STATSDEF_H_
#define GRAPH_STATS_STATSDEF_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

DECLARE_int32(slow_query_threshold_us);
DECLARE_bool(enable_space_level_metrics);

namespace nebula {

// query related
extern stats::CounterId kNumQueries;
extern stats::CounterId kNumActiveQueries;
extern stats::CounterId kNumSlowQueries;
extern stats::CounterId kNumQueryErrors;
extern stats::CounterId kNumQueryErrosLeaderChanges;
extern stats::CounterId kNumSentences;
extern stats::CounterId kQueryLatencyUs;
extern stats::CounterId kSlowQueryLatencyUs;

// session related
extern stats::CounterId kNumOpenedSessions;
extern stats::CounterId kNumAuthFailedSessions;
extern stats::CounterId kNumAuthFailedSessionsBadUserNamePassword;
extern stats::CounterId kNumAuthFailedSessionsOutOfMaxAllowed;

void initCounters();

}  // namespace nebula
#endif  // GRAPH_STATS_STATSDEF_H_
