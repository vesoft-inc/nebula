/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/stats/Metrics.h"

namespace nebula {
namespace stats {
Metrics* Metrics::instance_ = new Metrics;
void Metrics::registerCounter(const std::string& name) {
  counterMap_[name] = new Counter(name);
}
void Metrics::registerHistogram(const std::string& name,
                                int64_t bucketSize,
                                int64_t min,
                                int64_t max) {
  histogramMap_[name] = new Histogram(name, bucketSize, min, max);
}
void Metrics::registerGauge(const std::string& name, Gauge::Callback&& callback) {
  gaugeMap_[name] = new Gauge(name, std::move(callback));
}

void Metrics::counter(const std::string& name, int64_t value) {
  auto iter = counterMap_.find(name);
  CHECK(iter != counterMap_.end());
  iter->second->addValue(value);
}
void Metrics::counter(const std::string& name, const std::string& label, int64_t value) {
  auto iter = counterMap_.find(name);
  CHECK(iter != counterMap_.end());
  iter->second->addValue(label, value);
}
void Metrics::histogram(const std::string& name, int64_t value) {
  auto iter = histogramMap_.find(name);
  CHECK(iter != histogramMap_.end());
  iter->second->addValue(value);
}
void Metrics::histogram(const std::string& name, const std::string& label, int64_t value) {
  auto iter = histogramMap_.find(name);
  CHECK(iter != histogramMap_.end());
  iter->second->addValue(label, value);
}
void Metrics::readAll(MetricsReader& reader) {
  for (auto& [name, counter] : counterMap_) {
    counter->read(reader);
  }
  for (auto& [name, histogram] : histogramMap_) {
    histogram->read(reader);
  }
  for (auto& [name, gauge] : gaugeMap_) {
    gauge->read(reader);
  }
}

}  // namespace stats

}  // namespace nebula
