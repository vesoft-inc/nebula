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

    explicit Stats(const std::string& serverName, const std::string& moduleName);

public:
    static void addStatsValue(const Stats *stats, bool ok, int64_t latency = 0, uint32_t count = 1);

    int32_t getQpsStatId() const;

    int32_t getErrorQpsStatId() const;

    int32_t getLatencyStatId() const;

private:
    int32_t qpsStatId_{0};
    int32_t errorQpsStatId_{0};
    int32_t latencyStatId_{0};
};
}  // namespace stats
}  // namespace nebula

#endif  // COMMON_STATS_STATS_H_
