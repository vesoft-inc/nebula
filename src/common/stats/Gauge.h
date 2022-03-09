/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include <functional>
#include <mutex>

#include "common/stats/MetricsReader.h"
#include "folly/container/F14Map.h"
#include "folly/synchronization/Rcu.h"

namespace nebula {
namespace stats {
class Gauge {
 public:
  using Callback = std::function<std::string()>;
  Gauge() = default;
  Gauge(const std::string& name, Callback&& callback);
  bool addLabel(const std::string& label, Callback&& callback);
  bool removeLabel(const std::string& label);
  void read(MetricsReader& reader);

 private:
  struct Element {
    Element() = default;
    Element(const std::string& elementName,
            const std::string& elementLabel,
            Callback&& elementCallback);
    std::string name;
    std::string label;
    std::mutex mut;
    Gauge::Callback callback;
  };
  using LabelMap = folly::F14FastMap<std::string, Element*>;
  std::atomic<LabelMap*> labelMap_;
  std::mutex writeMut_;
  Element globalElement_;
  void readElement(MetricsReader& reader, const Element& ele);
};
}  // namespace stats
}  // namespace nebula
