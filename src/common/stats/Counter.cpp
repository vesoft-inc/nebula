/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/stats/Counter.h"

#include "folly/synchronization/Rcu.h"
namespace nebula {
namespace stats {
Counter::Counter(const std::string& name)
    : globalElement_(name,
                     "",
                     Series(60,
                            {
                                std::chrono::seconds(5),
                                std::chrono::seconds(60),
                                std::chrono::seconds(600),
                                std::chrono::seconds(3600),
                            })) {}
void Counter::addValue(int64_t value) {
  std::unique_lock<std::mutex> lck(globalElement_.mut);
  globalElement_.series.addValue(std::chrono::seconds(::nebula::time::WallClock::fastNowInSec()),
                                 value);
}
void Counter::addValue(const std::string& label, int64_t value) {
  bool needCreate = false;
  {
    folly::rcu_reader guard;
    auto& map = *labelMap_.load();
    auto iter = map.find(label);
    if (LIKELY(iter != map.end())) {
      auto& element = *(iter->second);
      std::unique_lock<std::mutex> lck(element.mut);
      element.series.addValue(std::chrono::seconds(::nebula::time::WallClock::fastNowInSec()),
                              value);
    } else {
      needCreate = true;
    }
  }
  if (UNLIKELY(needCreate)) {
    std::unique_lock<std::mutex> lck(writeMut_);
    auto originMap = labelMap_.load();
    auto iter = originMap->find(label);
    if (LIKELY(iter == originMap->end())) {
      auto newMap = new LabelMap(*originMap);
      auto element = new Element(globalElement_.name, label, globalElement_.series);
      element->series.addValue(std::chrono::seconds(::nebula::time::WallClock::fastNowInSec()),
                               value);
      newMap->insert({label, element});
      labelMap_.store(newMap);
      folly::rcu_retire(originMap);
    } else {
      auto& element = *(iter->second);
      std::unique_lock<std::mutex> elementLock(element.mut);
      element.series.addValue(std::chrono::seconds(::nebula::time::WallClock::fastNowInSec()),
                              value);
    }
  }
}
bool Counter::removeLabel(const std::string& label) {
  LabelMap* originMap = nullptr;
  Element* element = nullptr;
  {
    std::unique_lock<std::mutex> lck(writeMut_);
    originMap = labelMap_.load();
    auto iter = originMap->find(label);
    if (iter == originMap->end()) {
      return false;
    }
    element = iter->second;
    auto newMap = new LabelMap(*originMap);
    newMap->erase(label);
    labelMap_.store(newMap);
  }
  folly::synchronize_rcu();
  delete originMap;
  delete element;
  return true;
}
void Counter::read(MetricsReader& reader) {
  readElement(reader, globalElement_);
  folly::rcu_reader guard;
  auto& map = *labelMap_.load();
  for (auto& [name, ele] : map) {
    readElement(reader, *ele);
  }
}
void Counter::readElement(MetricsReader& reader, const Element& element) {
  std::string prefix = element.name;
  if (!element.label.empty()) {
    prefix += "{" + element.label + "}";
  }
  auto& series = element.series;
  reader.append(prefix + ".avg" + ".5", series.template avg<int64_t>(0))
      .append(prefix + ".avg" + ".60", series.template avg<int64_t>(1))
      .append(prefix + ".avg" + ".600", series.template avg<int64_t>(2))
      .append(prefix + ".avg" + ".3600", series.template avg<int64_t>(3));
  reader.append(prefix + ".sum" + ".5", series.sum(0))
      .append(prefix + ".sum" + ".60", series.sum(1))
      .append(prefix + ".sum" + ".600", series.sum(2))
      .append(prefix + ".sum" + ".3600", series.sum(3));
  reader.append(prefix + ".rate" + ".5", series.template rate(0))
      .append(prefix + ".rate" + ".60", series.template rate(1))
      .append(prefix + ".rate" + ".600", series.template rate(2))
      .append(prefix + ".rate" + ".3600", series.template rate(3));
  reader.append(prefix + ".count" + ".5", series.count(0))
      .append(prefix + ".count" + ".60", series.count(1))
      .append(prefix + ".count" + ".600", series.count(2))
      .append(prefix + ".count" + ".3600", series.count(3));
}
Counter::Element::Element(const std::string& elementName,
                          const std::string& elementLabel,
                          const Counter::Series& elementSeries)
    : name(elementName), label(elementLabel), series(elementSeries) {
  this->series.clear();
}
}  // namespace stats
}  // namespace nebula
