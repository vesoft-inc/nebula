/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_STATS_STATSDEF_H_
#define GRAPH_STATS_STATSDEF_H_

#include "common/base/Base.h"
#include "common/metrics/Metric.h"
#include "common/stats/StatsManager.h"

DECLARE_int32(slow_query_threshold_us);

namespace nebula {
namespace metric {

extern CounterVec kNumQueries;
extern CounterVec kNumSlowQueries;
extern CounterVec kNumQueryErrors;
extern HistogramVec kQueryLatencyUs;
extern HistogramVec kSlowQueryLatencyUs;

void initMetrics();

}  // namespace metric
}  // namespace nebula
#endif  // GRAPH_STATS_STATSDEF_H_
