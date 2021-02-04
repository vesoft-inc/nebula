/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "StatsDef.h"
#include "common/stats/StatsManager.h"

DEFINE_int32(slow_query_threshold_us, 200000,
             "Any query slower than this threshold value will be considered"
             " as a slow query");


namespace nebula {

stats::CounterId kNumQueries;
stats::CounterId kNumSlowQueries;
stats::CounterId kNumQueryErrors;
stats::CounterId kQueryLatencyUs;
stats::CounterId kSlowQueryLatencyUs;

void initCounters() {
    kNumQueries = stats::StatsManager::registerStats("num_queries", "rate, sum");
    kNumSlowQueries = stats::StatsManager::registerStats("num_slow_queries", "rate, sum");
    kNumQueryErrors = stats::StatsManager::registerStats("num_query_errors", "rate, sum");
    kQueryLatencyUs = stats::StatsManager::registerHisto(
        "query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
    kSlowQueryLatencyUs = stats::StatsManager::registerHisto(
        "slow_query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999");
}

}  // namespace nebula
