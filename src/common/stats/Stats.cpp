/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "stats/StatsManager.h"
#include "stats/Stats.h"

namespace nebula {
namespace stats {

Stats::Stats(const std::string& name) {
    qpsStatId_ = StatsManager::registerStats(name + "_qps");
    errorQpsStatId_ = StatsManager::registerStats(name + "_error_qps");
    latencyStatId_ = StatsManager::registerHisto(name + "_latency", 1, 1, 100);
}

// static
void Stats::addStatsValue(Stats *stats, bool ok, int64_t latency) {
    if (stats == nullptr) {
        return;
    }
    if (ok && stats->getQpsStatId() != 0) {
        StatsManager::addValue(stats->getQpsStatId(), 1);
    }
    if (!ok && stats->getErrorQpsStatId() != 0) {
        StatsManager::addValue(stats->getErrorQpsStatId(), 1);
    }
    if (stats->getLatencyStatId() != 0) {
        StatsManager::addValue(stats->getLatencyStatId(), latency);
    }
}

int32_t Stats::getQpsStatId() {
   return qpsStatId_;
}

int32_t Stats::getErrorQpsStatId() {
    return errorQpsStatId_;
}

int32_t Stats::getLatencyStatId() {
    return latencyStatId_;
}

}  // namespace stats
}  // namespace nebula

