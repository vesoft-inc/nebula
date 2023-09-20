/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/stats/KVStats.h"

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

namespace nebula {

stats::CounterId kCommitLogLatencyUs;
stats::CounterId kCommitSnapshotLatencyUs;
stats::CounterId kAppendWalLatencyUs;
stats::CounterId kReplicateLogLatencyUs;
stats::CounterId kAppendLogLatencyUs;
stats::CounterId kTransferLeaderLatencyUs;
stats::CounterId kNumStartElect;
stats::CounterId kNumGrantVotes;
stats::CounterId kNumSendSnapshot;
stats::CounterId kFollowerCommitLogLatencyUs;

void initKVStats() {
  kCommitLogLatencyUs = stats::StatsManager::registerHisto(
      "commit_log_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kCommitSnapshotLatencyUs = stats::StatsManager::registerHisto(
      "commit_snapshot_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kAppendWalLatencyUs = stats::StatsManager::registerHisto(
      "append_wal_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kReplicateLogLatencyUs = stats::StatsManager::registerHisto(
      "replicate_log_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kAppendLogLatencyUs = stats::StatsManager::registerHisto(
      "append_log_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kTransferLeaderLatencyUs = stats::StatsManager::registerHisto(
      "transfer_leader_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kNumStartElect = stats::StatsManager::registerStats("num_start_elect", "rate, sum");
  kNumGrantVotes = stats::StatsManager::registerStats("num_grant_votes", "rate, sum");
  kNumSendSnapshot = stats::StatsManager::registerStats("num_send_snapshot", "rate, sum");
  kFollowerCommitLogLatencyUs = stats::StatsManager::registerHisto(
      "follower_commit_log_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
}

}  // namespace nebula
