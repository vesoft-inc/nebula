/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/RWSpinLock.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/stats/TimeseriesHistogram.h>
#include <rocksdb/statistics.h>
#include <sys/types.h>

#include <cstdint>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/HostAddr.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace metric {

using std::chrono::seconds;
using VT = int64_t;
using StatsType = folly::MultiLevelTimeSeries<VT>;
enum class StatsMethod { SUM = 1, COUNT, AVG, RATE };

void parseStats(const folly::StringPiece stats,
                std::vector<StatsMethod>& methods,
                std::vector<std::pair<std::string, double>>& percentiles);

bool strToPct(folly::StringPiece part, double& pct);

class Metric {
 public:
  Metric() = default;
  virtual ~Metric() = default;
  virtual std::string name() const { return "fafd"; }
  // virtual std::string to_string() const = 0;
};

struct CounterOpts {
  CounterOpts(const std::string& name, const std::string& stats) : name(name), stats(stats) {}
  std::string name;
  std::string stats;  // eg. "rate,sum"
};

class Counter : public Metric {
 public:
  explicit Counter(const CounterOpts& opts)
      : name_(opts.name),
        counter_(60,
                 std::initializer_list<StatsType::Duration>(
                     {seconds(5), seconds(60), seconds(600), seconds(3600)})) {
    std::vector<std::pair<std::string, double>> tmp;
    parseStats(opts.stats, methods_, tmp);
  }

  void addValue(VT v = 1) {
    std::lock_guard<std::mutex> g(lock_);
    counter_.addValue(seconds(time::WallClock::fastNowInSec()), v);
  }

 private:
  std::string name_;
  std::vector<StatsMethod> methods_;
  StatsType counter_;
  std::mutex lock_;
};

struct HistogramOpts {
  HistogramOpts(const std::string& name, VT bucketSize, VT min, VT max, const std::string& stats)
      : name(name), bucketSize(bucketSize), min(min), max(max), stats(stats) {}
  std::string name;
  VT bucketSize;
  VT min;
  VT max;
  std::string stats;  // eg. "avg, p75, p99"
};

class Histogram : public Metric {
  using HistogramType = folly::TimeseriesHistogram<VT>;

 public:
  explicit Histogram(const HistogramOpts& opts)
      : hist_(opts.bucketSize,
              opts.min,
              opts.max,
              StatsType(60, {seconds(5), seconds(60), seconds(600), seconds(3600)})) {
    parseStats(opts.stats, methods_, percentiles_);
  }

  void addValue(VT v) {
    std::lock_guard<std::mutex> g(lock_);
    hist_.addValue(seconds(time::WallClock::fastNowInSec()), v);
  }

 private:
  folly::TimeseriesHistogram<VT> hist_;
  std::vector<StatsMethod> methods_;
  std::vector<std::pair<std::string, double>> percentiles_;
  std::mutex lock_;
};

using LabelName = std::string;
using LabelValue = std::string;
using LabelNames = std::vector<LabelName>;
using LabelValues = std::vector<LabelValue>;

struct LabelValuesHash {
  size_t operator()(const LabelValues& v) const {
    std::string s = folly::join("", v);
    return std::hash<std::string>()(s);
  }
};

template <typename T>
class MetricVec {
 public:
  MetricVec(const std::string& name, const LabelNames& labelNames, std::function<T*()> addMetric)
      : name_(name), labelNames_(labelNames), addMetric_(addMetric) {}

  T& getOrCreateMetricWithLabelValues(const LabelValues& labelValues) {
    folly::RWSpinLock::WriteHolder wh(lock_);

    // Get the metric if it already exists
    auto it = metricMap_.find(labelValues);
    if (it != metricMap_.end()) {
      return *it->second;
    }

    // Create a new one if it doesn't exist
    auto it2 = metricMap_.emplace(labelValues, addMetric_());
    CHECK(it2.second);
    return *it2.first->second;
  }

  void removeMetricWithLabelValues(const LabelValues& labelValues) {
    folly::RWSpinLock::WriteHolder wh(lock_);
    metricMap_.erase(labelValues);
  }

 private:
  std::unordered_map<LabelValues, std::unique_ptr<T>, LabelValuesHash> metricMap_;

  const std::string name_;
  const LabelNames labelNames_;
  std::function<T*()> addMetric_;
  folly::RWSpinLock lock_;
};

class CounterVec : public Metric {
 public:
  CounterVec(const CounterOpts& opts, const LabelNames& labelNames)
      : metricVec_(opts.name, labelNames, [opts]() { return new Counter(opts); }) {}

  Counter& withLabelValues(const LabelValues& labelValues) {
    return metricVec_.getOrCreateMetricWithLabelValues(labelValues);
  }

  void removeMetricWithLabelValues(const LabelValues& labelValues) {
    metricVec_.removeMetricWithLabelValues(labelValues);
  }

 private:
  MetricVec<Counter> metricVec_;
};

class HistogramVec : public Metric {
 public:
  HistogramVec(const HistogramOpts& opts, const LabelNames& labelNames)
      : metricVec_(opts.name, labelNames, [opts]() { return new Histogram(opts); }) {}

  Histogram& withLabelValues(const LabelValues& labelValues) {
    return metricVec_.getOrCreateMetricWithLabelValues(labelValues);
  }

  void removeMetricWithLabelValues(const LabelValues& labelValues) {
    metricVec_.removeMetricWithLabelValues(labelValues);
  }

 private:
  MetricVec<Histogram> metricVec_;
};

class MetricRegistry {
 public:
  static void registerMetric(const Metric* metric) {
    auto& registry = instance();
    folly::RWSpinLock::WriteHolder wh(registry.lock_);
    registry.metrics_.try_emplace(metric->name(), metric);
  }

  static void unRegisterMetric(const Metric* metric) {
    auto& registry = instance();
    folly::RWSpinLock::WriteHolder wh(registry.lock_);
    registry.metrics_.erase(metric->name());
  }

  static const Metric* getMetric(const std::string& name) {
    auto& registry = instance();
    folly::RWSpinLock::ReadHolder rh(registry.lock_);
    auto it = registry.metrics_.find(name);
    if (it == registry.metrics_.end()) {
      return nullptr;
    }
    return it->second;
  }

  static void readAllValue(folly::dynamic& vals);

 private:
  static MetricRegistry& instance() {
    static MetricRegistry registry;
    return registry;
  }

  MetricRegistry() = default;

 private:
  folly::RWSpinLock lock_;
  std::unordered_map<std::string, const Metric*> metrics_;
};

}  // namespace metric
}  // namespace nebula

// namespace std {

// // Inject a customized hash function
// template <>
// struct hash<nebula::metric::LabelValues> {
//   std::size_t operator()(const nebula::metric::LabelValues& h) const noexcept;
// };

// }  // namespace std
