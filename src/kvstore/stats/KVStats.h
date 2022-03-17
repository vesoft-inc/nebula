/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_STATS_KVSTATS_H
#define KVSTORE_STATS_KVSTATS_H

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

namespace nebula {

// Raft related stats
extern stats::CounterId kCommitLogLatencyUs;
extern stats::CounterId kCommitSnapshotLatencyUs;
extern stats::CounterId kAppendWalLatencyUs;
extern stats::CounterId kReplicateLogLatencyUs;
extern stats::CounterId kAppendLogLatencyUs;
extern stats::CounterId kTransferLeaderLatencyUs;
extern stats::CounterId kNumStartElect;
extern stats::CounterId kNumGrantVotes;
extern stats::CounterId kNumSendSnapshot;

void initKVStats();

}  // namespace nebula
#endif
