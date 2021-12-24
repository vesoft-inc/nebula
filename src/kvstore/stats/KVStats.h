/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

namespace nebula {

extern stats::CounterId kCommitLogLatencyUs;
extern stats::CounterId kCommitSnapshotLatencyUs;
extern stats::CounterId kTransferLeaderLatencyUs;
extern stats::CounterId kNumRaftVotes;

void initKVStats();

}  // namespace nebula
