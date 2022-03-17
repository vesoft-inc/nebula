/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTIS_META_STATS_METACLIENTSTATS_H
#define CLIENTIS_META_STATS_METACLIENTSTATS_H
#include "common/stats/StatsManager.h"

namespace nebula {

extern stats::CounterId kNumRpcSentToMetad;
extern stats::CounterId kNumRpcSentToMetadFailed;

void initMetaClientStats();

}  // namespace nebula
#endif
