/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GRAPHSTATS_H
#define GRAPH_GRAPHSTATS_H

#include "stats/StatsManager.h"

namespace nebula {
namespace graph {

class GraphStats final {
public:
    GraphStats() {
        qpsStatId_ = stats::StatsManager::registerStats("graph_qps");
        sQpsStatId_ = stats::StatsManager::registerStats("storage_qps");
        mQpsStatId_ = stats::StatsManager::registerStats("meta_qps");
        errorGQpsStatId_ = stats::StatsManager::registerStats("graph_error_qps");
        errorMQpsStatId_ = stats::StatsManager::registerStats("meta_error_qps");
        errorSQpsStatId_ = stats::StatsManager::registerStats("storage_error_qps");
        sLatencyStatId_ = stats::StatsManager::registerHisto("storage_latency", 1, 1, 100);
        mLatencyStatId_ = stats::StatsManager::registerHisto("meta_latency", 1, 1, 100);
        wLatencyStatId_ = stats::StatsManager::registerHisto("whole_latency", 1, 1, 100);
    }

    ~GraphStats() = default;


public:
    int32_t getQpsId() { return qpsStatId_; }
    int32_t getSQpsId() { return sQpsStatId_; }
    int32_t getMQpsId() { return mQpsStatId_; }
    int32_t getErrorGQpsId() { return errorGQpsStatId_; }
    int32_t getErrorMQpsId() { return errorMQpsStatId_; }
    int32_t getErrorSQpsId() { return errorSQpsStatId_; }
    int32_t getWLatencyId() { return wLatencyStatId_; }
    int32_t getMLatencyId() { return mLatencyStatId_; }
    int32_t getSLatencyId() { return sLatencyStatId_; }


private:
    int32_t qpsStatId_{0};             // successful graph qps
    int32_t sQpsStatId_{0};            // successful storage qps
    int32_t mQpsStatId_{0};            // successful meta qps
    int32_t errorGQpsStatId_{0};       // the error qps, error handle in graphd
    int32_t errorMQpsStatId_{0};       // the error qps, error handle in metad
    int32_t errorSQpsStatId_{0};       // the error qps, error handle in storaged
    int32_t sLatencyStatId_{0};        // the latency from storageClient
    int32_t mLatencyStatId_{0};        // the latency from metaClient
    int32_t wLatencyStatId_{0};        // the whole latency of nGQL `graphd -> storaged -> graphd`
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_GRAPHSTATS_H
