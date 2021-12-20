/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/stats/StatsDef.h"

#include <thrift/lib/thrift/gen-cpp2/RpcMetadata_types.h>

#include "common/base/Base.h"
#include "common/metrics/Metric.h"
#include "common/stats/StatsManager.h"

DEFINE_int32(slow_query_threshold_us,
             200000,
             "Any query slower than this threshold value will be considered"
             " as a slow query");

namespace nebula {
namespace metric {

CounterVec kNumQueries(CounterOpts("num_queries", "rate, sum"), {"space"});
CounterVec kNumSlowQueries(CounterOpts("num_slow_queries", "rate, sum"), {"space"});
CounterVec kNumQueryErrors(CounterOpts("num_query_errors", "rate, sum"), {"space"});
HistogramVec kQueryLatencyUs(
    HistogramOpts("query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999"), {"space"});
HistogramVec kSlowQueryLatencyUs(
    HistogramOpts("slow_query_latency_us", 1000, 0, 2000, "avg, p75, p95, p99, p999"), {"space"});

void initMetrics() {
  MetricRegistry::registerMetric(&kNumQueries);
  MetricRegistry::registerMetric(&kNumSlowQueries);
  MetricRegistry::registerMetric(&kNumQueryErrors);
  MetricRegistry::registerMetric(&kQueryLatencyUs);
  MetricRegistry::registerMetric(&kSlowQueryLatencyUs);
}

}  // namespace metric
}  // namespace nebula
