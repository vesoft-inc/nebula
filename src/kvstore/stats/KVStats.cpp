/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/stats/KVStats.h"

#include <memory>  // for allocator

#include "common/stats/StatsManager.h"  // for CounterId, StatsManager

namespace nebula {

stats::CounterId kCommitLogLatencyUs;
stats::CounterId kCommitSnapshotLatencyUs;
stats::CounterId kTransferLeaderLatencyUs;
stats::CounterId kNumRaftVotes;

void initKVStats() {
  kCommitLogLatencyUs = stats::StatsManager::registerHisto(
      "commit_log_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kCommitSnapshotLatencyUs = stats::StatsManager::registerHisto(
      "commit_snapshot_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kTransferLeaderLatencyUs = stats::StatsManager::registerHisto(
      "transfer_leader_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
  kNumRaftVotes = stats::StatsManager::registerStats("num_raft_votes", "rate, sum");
}

}  // namespace nebula
