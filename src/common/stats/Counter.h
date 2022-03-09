/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include <chrono>

#include "common/stats/MetricsReader.h"
#include "common/time/WallClock.h"
#include "folly/Likely.h"
#include "folly/container/F14Map.h"
#include "folly/stats/MultiLevelTimeSeries.h"
namespace nebula {
namespace stats {
// sum: 时间段内总和
// count: 时间段内次数
// rate: sum/时间段大小
// avg: sum/count
class Counter {
 public:
  explicit Counter(const std::string& name);
  void addValue(int64_t value);
  void addValue(const std::string& label, int64_t value);
  bool removeLabel(const std::string& label);
  void read(MetricsReader& reader);

 private:
  using Series = folly::MultiLevelTimeSeries<int64_t>;
  struct Element {
    Element() = default;
    Element(const std::string& elementName,
            const std::string& elementLabel,
            const Counter::Series& elementSeries);
    std::string name;
    std::string label;
    std::mutex mut;
    Counter::Series series;
  };
  using LabelMap = folly::F14FastMap<std::string, Element*>;
  std::atomic<LabelMap*> labelMap_;
  std::mutex writeMut_;
  Element globalElement_;
  void readElement(MetricsReader& reader, const Element& element);
};
}  // namespace stats
}  // namespace nebula
