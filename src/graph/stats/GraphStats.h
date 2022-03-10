/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_STATS_GRAPHSTATS_H
#define GRAPH_STATS_GRAPHSTATS_H

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

DECLARE_int32(slow_query_threshold_us);
DECLARE_bool(enable_space_level_metrics);

namespace nebula {

// Query
// A sequential sentence is treated as one query
extern stats::CounterId kNumQueries;
extern stats::CounterId kNumActiveQueries;
extern stats::CounterId kNumSlowQueries;
extern stats::CounterId kNumQueryErrors;
extern stats::CounterId kNumQueryErrorsLeaderChanges;
// A sequential sentence is treated as multiple sentences seperated by `;`
extern stats::CounterId kNumSentences;
extern stats::CounterId kQueryLatencyUs;
extern stats::CounterId kSlowQueryLatencyUs;
extern stats::CounterId kNumKilledQueries;
extern stats::CounterId kNumQueriesHitMemoryWatermark;

extern stats::CounterId kOptimizerLatencyUs;

// Executor
extern stats::CounterId kNumAggregateExecutors;
extern stats::CounterId kNumSortExecutors;
extern stats::CounterId kNumIndexScanExecutors;

// Server client traffic
// extern stats::CounterId kReceivedBytes;
// extern stats::CounterId kSentBytes;

// Session
extern stats::CounterId kNumOpenedSessions;
extern stats::CounterId kNumAuthFailedSessions;
extern stats::CounterId kNumAuthFailedSessionsBadUserNamePassword;
extern stats::CounterId kNumAuthFailedSessionsOutOfMaxAllowed;
extern stats::CounterId kNumActiveSessions;
extern stats::CounterId kNumReclaimedExpiredSessions;

void initGraphStats();

}  // namespace nebula
#endif
