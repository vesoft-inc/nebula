/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/stats/Gauge.h"

namespace nebula {
namespace stats {
Gauge::Gauge(const std::string& name, Callback&& callback)
    : globalElement_(name, "", std::move(callback)) {}
bool Gauge::addLabel(const std::string& label, Callback&& callback) {
  std::unique_lock<std::mutex> lck(writeMut_);
  auto originMap = labelMap_.load();
  auto newMap = new LabelMap(*originMap);
  (*newMap)[label] = new Element(globalElement_.name, label, std::move(callback));
  labelMap_.store(newMap);
  folly::rcu_retire(originMap);
  return true;
}
bool Gauge::removeLabel(const std::string& label) {
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
  }
  folly::synchronize_rcu();
  delete originMap;
  delete element;
  return true;
}
void Gauge::read(MetricsReader& reader) {
  readElement(reader, globalElement_);
  folly::rcu_reader guard;
  auto& map = *labelMap_.load();
  for (auto& [label, ele] : map) {
    readElement(reader, *ele);
  }
}
Gauge::Element::Element(const std::string& elementName,
                        const std::string& elementLabel,
                        Callback&& elementCallback)
    : name(elementName), label(elementLabel), callback(std::move(elementCallback)) {}
void Gauge::readElement(MetricsReader& reader, const Element& ele) {
  std::string prefix = ele.name;
  if (!ele.label.empty()) {
    prefix += "{" + ele.label + "}";
  }
  reader.append(prefix, ele.callback());
}
}  // namespace stats

}  // namespace nebula
