/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_STATS_STORAGECLIENTSTATS_H
#define CLIENTS_STORAGE_STATS_STORAGECLIENTSTATS_H

#include "common/stats/StatsManager.h"

namespace nebula {

extern stats::CounterId kNumRpcSentToStoraged;
extern stats::CounterId kNumRpcSentToStoragedFailed;

void initStorageClientStats();

}  // namespace nebula
#endif
