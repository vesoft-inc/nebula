/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_STATS_STATS_H_
#define COMMON_STATS_STATS_H_

#include "stats/StatsManager.h"

namespace nebula {
namespace stats {

class Stats final {
public:
    Stats() = default;
    ~Stats() = default;

    explicit Stats(const std::string& name);

public:
    static void addStatsValue(Stats *stats, bool ok, int64_t latency = 0);

    int32_t getQpsStatId();

    int32_t getErrorQpsStatId();

    int32_t getLatencyStatId();

private:
    int32_t qpsStatId_{0};
    int32_t errorQpsStatId_{0};
    int32_t latencyStatId_{0};
};
}  // namespace stats
}  // namespace nebula

#endif  // COMMON_STATS_STATS_H_
