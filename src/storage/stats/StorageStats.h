/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_STATS_STORAGESTATS_H
#define STORAGE_STATS_STORAGESTATS_H

#include "common/stats/StatsManager.h"

namespace nebula {

extern stats::CounterId kNumEdgesInserted;
extern stats::CounterId kNumVerticesInserted;
extern stats::CounterId kNumEdgesDeleted;
extern stats::CounterId kNumTagsDeleted;
extern stats::CounterId kNumVerticesDeleted;

/**
 * @brief Init storage statistic points for storage/meta client/kv
 *
 */
void initStorageStats();

}  // namespace nebula
#endif
