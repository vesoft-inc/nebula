/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/stats/StatsDef.h"

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

DEFINE_int32(slow_query_threshold_us,
             200000,
             "Any query slower than this threshold value will be considered"
             " as a slow query");
DEFINE_bool(enable_space_level_metrics, false, "Whether to enable space level metrircs");

namespace nebula {

stats::CounterId kNumQueries;
stats::CounterId kNumActiveQueries;
stats::CounterId kNumSlowQueries;
stats::CounterId kNumQueryErrors;
stats::CounterId kNumQueryErrosLeaderChanges;
stats::CounterId kNumSentences;
stats::CounterId kQueryLatencyUs;
stats::CounterId kSlowQueryLatencyUs;
stats::CounterId kNumKilledQueries;

stats::CounterId kOptimizerLatencyUs;

stats::CounterId kNumAggregateExecutors;
stats::CounterId kNumSortExecutors;
stats::CounterId kNumIndexScanExecutors;
stats::CounterId kNumOomExecutors;

stats::CounterId kReceivedBytes;
stats::CounterId kSentBytes;

stats::CounterId kNumOpenedSessions;
stats::CounterId kNumAuthFailedSessions;
stats::CounterId kNumAuthFailedSessionsBadUserNamePassword;
stats::CounterId kNumAuthFailedSessionsOutOfMaxAllowed;
stats::CounterId kNumActiveSessions;
stats::CounterId kNumReclaimedExpiredSessions;

void initCounters() {
  kNumQueries = stats::StatsManager::registerStats("num_queries", "rate, sum");
  kNumActiveQueries = stats::StatsManager::registerStats("num_active_queries", "sum");
  kNumSlowQueries = stats::StatsManager::registerStats("num_slow_queries", "rate, sum");
  kNumSentences = stats::StatsManager::registerStats("num_sentences", "rate, sum");
  kNumQueryErrors = stats::StatsManager::registerStats("num_query_errors", "rate, sum");
  kNumQueryErrosLeaderChanges =
      stats::StatsManager::registerStats("num_query_errors_leader_changes", "rate, sum");
  kQueryLatencyUs = stats::StatsManager::registerHisto(
      "query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kSlowQueryLatencyUs = stats::StatsManager::registerHisto(
      "slow_query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kNumKilledQueries = stats::StatsManager::registerStats("num_killed_queries", "rate, sum");

  kOptimizerLatencyUs = stats::StatsManager::registerHisto(
      "optimizer_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");

  kReceivedBytes = stats::StatsManager::registerHisto(
      "received_bytes", 1000, 1, 4194304, "avg, p75, p95, p99, p999");  // 1 Byte ~ 4194304 Bytes
  kSentBytes = stats::StatsManager::registerHisto(
      "sent_bytes", 1000, 1, 4294967296, "avg, p75, p95, p99, p999");  // 1 Byte ~ 4 GiB

  kNumAggregateExecutors =
      stats::StatsManager::registerStats("num_aggregate_executors", "rate, sum");
  kNumSortExecutors = stats::StatsManager::registerStats("num_sort_executors", "rate, sum");
  kNumIndexScanExecutors =
      stats::StatsManager::registerStats("num_indexscan_executors", "rate, sum");
  kNumOomExecutors = stats::StatsManager::registerStats("num_oom_executors", "rate, sum");

  kNumOpenedSessions = stats::StatsManager::registerStats("num_opened_sessions", "rate, sum");
  kNumAuthFailedSessions =
      stats::StatsManager::registerStats("num_auth_failed_sessions", "rate, sum");
  kNumAuthFailedSessionsBadUserNamePassword = stats::StatsManager::registerStats(
      "num_auth_failed_sessions_bad_username_password", "rate, sum");
  kNumAuthFailedSessionsOutOfMaxAllowed = stats::StatsManager::registerStats(
      "num_auth_failed_sessions_out_of_max_allowed", "rate, sum");
  kNumActiveSessions = stats::StatsManager::registerStats("num_active_sessions", "sum");
  kNumReclaimedExpiredSessions =
      stats::StatsManager::registerStats("num_reclaimed_expired_sessions", "rate, sum");
}

}  // namespace nebula
