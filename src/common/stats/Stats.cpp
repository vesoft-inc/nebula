/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "stats/StatsManager.h"
#include "stats/Stats.h"

DEFINE_int32(histogram_bucketSize, 1000, "The width of each bucket");
DEFINE_uint32(histogram_min, 1, "The smallest value for the bucket range");
DEFINE_uint32(histogram_max, 1000 * 1000, "The largest value for the bucket range");

namespace nebula {
namespace stats {

Stats::Stats(const std::string& serverName, const std::string& moduleName) {
    qpsStatId_ = StatsManager::registerStats(serverName + "_" + moduleName + "_qps");
    errorQpsStatId_ = StatsManager::registerStats(serverName + "_" + moduleName + "_error_qps");
    latencyStatId_ = StatsManager::registerHisto(serverName + "_" + moduleName + "_latency",
            FLAGS_histogram_bucketSize, FLAGS_histogram_min, FLAGS_histogram_max);
}

// static
void Stats::addStatsValue(const Stats *stats, bool ok, int64_t latency, uint32_t count) {
    if (stats == nullptr) {
        return;
    }
    if (ok && stats->getQpsStatId() != 0) {
        StatsManager::addValue(stats->getQpsStatId(), count);
    }
    if (!ok && stats->getErrorQpsStatId() != 0) {
        StatsManager::addValue(stats->getErrorQpsStatId(), count);
    }
    if (stats->getLatencyStatId() != 0) {
        StatsManager::addValue(stats->getLatencyStatId(), latency);
    }
}

int32_t Stats::getQpsStatId() const {
   return qpsStatId_;
}

int32_t Stats::getErrorQpsStatId() const {
    return errorQpsStatId_;
}

int32_t Stats::getLatencyStatId() const {
    return latencyStatId_;
}

}  // namespace stats
}  // namespace nebula

