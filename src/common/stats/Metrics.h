/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include <array>
#include <functional>

#include "common/stats/Counter.h"
#include "common/stats/Gauge.h"
#include "common/stats/Histogram.h"
#include "folly/container/F14Map.h"
namespace nebula {
namespace stats {
class Metrics {
 public:
  static Metrics& getInstance() {
    return *instance_;
  }
  ~Metrics() = default;

  // register
  void registerCounter(const std::string& name);
  void registerHistogram(const std::string& name, int64_t bucketSize, int64_t min, int64_t max);
  void registerGauge(const std::string& name, Gauge::Callback&& callback);

  // write
  void counter(const std::string& name, int64_t value = 1);
  void counter(const std::string& name, const std::string& labels, int64_t value = 1);
  void histogram(const std::string& name, int64_t value);
  void histogram(const std::string& name, const std::string& labels, int64_t value);
  void gauge(const std::string& name, const std::string& label, Gauge::Callback&& callback);

  // read
  void readAll(MetricsReader& reader);

 private:
  static Metrics* instance_;
  folly::F14FastMap<std::string, Counter*> counterMap_;
  folly::F14FastMap<std::string, Histogram*> histogramMap_;
  folly::F14FastMap<std::string, Gauge*> gaugeMap_;
};
}  // namespace stats
}  // namespace nebula
