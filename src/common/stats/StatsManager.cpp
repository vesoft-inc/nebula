/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/stats/StatsManager.h"

#include <folly/Range.h>
#include <folly/String.h>
#include <glog/logging.h>

#include "common/base/Base.h"

namespace nebula {
namespace stats {

// static
StatsManager& StatsManager::get() {
  static StatsManager smInst;
  return smInst;
}

// static
void StatsManager::setDomain(folly::StringPiece domain) {
  get().domain_ = domain.toString();
}

// static
void StatsManager::setReportInfo(HostAddr addr, int32_t interval) {
  auto& sm = get();
  sm.collectorAddr_ = addr;
  sm.interval_ = interval;
}

// static
void StatsManager::parseStats(const folly::StringPiece stats,
                              std::vector<StatsMethod>& methods,
                              std::vector<std::pair<std::string, double>>& percentiles) {
  std::vector<std::string> parts;
  folly::split(",", stats, parts, true);

  for (auto& part : parts) {
    // Now check the statistic method
    std::string trimmedPart = folly::trimWhitespace(part).toString();
    folly::toLowerAscii(trimmedPart);
    if (trimmedPart == "sum") {
      methods.push_back(StatsMethod::SUM);
    } else if (trimmedPart == "count") {
      methods.push_back(StatsMethod::COUNT);
    } else if (trimmedPart == "avg") {
      methods.push_back(StatsMethod::AVG);
    } else if (trimmedPart == "rate") {
      methods.push_back(StatsMethod::RATE);
    } else if (trimmedPart[0] == 'p') {
      // Percentile
      double pct;
      if (strToPct(trimmedPart, pct)) {
        percentiles.emplace_back(trimmedPart, pct);
      } else {
        LOG(ERROR) << "\"" << trimmedPart << "\" is not a valid percentile form";
      }
    } else {
      LOG(ERROR) << "Unsupported statistic method \"" << trimmedPart << "\"";
    }
  }
}

// static
CounterId StatsManager::registerStats(folly::StringPiece counterName, std::string stats) {
  std::vector<StatsMethod> methods;
  std::vector<std::pair<std::string, double>> percentiles;
  parseStats(stats, methods, percentiles);
  return registerStats(counterName, methods);
}

CounterId StatsManager::registerStats(folly::StringPiece counterName,
                                      std::vector<StatsMethod> methods) {
  using std::chrono::seconds;

  auto& sm = get();

  std::string name = counterName.toString();
  folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
  auto it = sm.nameMap_.find(name);
  if (it != sm.nameMap_.end()) {
    DCHECK(!it->second.id_.isHisto());
    VLOG(2) << "The counter \"" << name << "\" already exists";
    it->second.methods_ = methods;
    return it->second.id_;
  }

  // Insert the Stats
  sm.stats_.emplace(
      counterName,
      std::make_pair(std::make_unique<std::mutex>(),
                     std::make_unique<StatsType>(
                         60,
                         std::initializer_list<StatsType::Duration>(
                             {seconds(5), seconds(60), seconds(600), seconds(3600)}))));
  std::string index(counterName);
  auto it2 = sm.nameMap_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::move(name)),
      std::forward_as_tuple(
          index, std::move(methods), std::vector<std::pair<std::string, double>>(), false));

  VLOG(1) << "Registered stats " << counterName.toString();
  return it2.first->second.id_;
}

// static
CounterId StatsManager::registerHisto(folly::StringPiece counterName,
                                      StatsManager::VT bucketSize,
                                      StatsManager::VT min,
                                      StatsManager::VT max,
                                      std::string stats) {
  std::vector<StatsMethod> methods;
  std::vector<std::pair<std::string, double>> percentiles;
  parseStats(stats, methods, percentiles);
  return registerHisto(counterName, bucketSize, min, max, methods, percentiles);
}

// static
CounterId StatsManager::registerHisto(folly::StringPiece counterName,
                                      StatsManager::VT bucketSize,
                                      StatsManager::VT min,
                                      StatsManager::VT max,
                                      std::vector<StatsMethod> methods,
                                      std::vector<std::pair<std::string, double>> percentiles) {
  using std::chrono::seconds;
  auto& sm = get();
  std::string name = counterName.toString();
  folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
  auto it = sm.nameMap_.find(name);
  if (it != sm.nameMap_.end()) {
    DCHECK(it->second.id_.isHisto());
    VLOG(2) << "The counter \"" << name << "\" already exists";
    it->second.methods_ = methods;
    it->second.percentiles_ = percentiles;
    return it->second.id_;
  }

  // Insert the Histogram
  sm.histograms_.emplace(
      counterName,
      std::make_pair(std::make_unique<std::mutex>(),
                     std::make_unique<HistogramType>(
                         bucketSize,
                         min,
                         max,
                         StatsType(60, {seconds(5), seconds(60), seconds(600), seconds(3600)}))));
  std::string index(counterName);
  auto it2 = sm.nameMap_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::move(name)),
      std::forward_as_tuple(
          index, std::move(methods), std::move(percentiles), true, bucketSize, min, max));

  VLOG(1) << "Registered histogram " << counterName.toString() << " [bucketSize: " << bucketSize
          << ", min value: " << min << ", max value: " << max << "]";
  return it2.first->second.id_;
}

// static
CounterId StatsManager::counterWithLabels(const CounterId& id,
                                          const std::vector<LabelPair>& labels) {
  using std::chrono::seconds;

  auto& sm = get();
  auto index = id.index();
  CHECK(!labels.empty());
  std::string newIndex;
  newIndex.append(index);
  newIndex.append("{");
  for (auto& [k, v] : labels) {
    newIndex.append(k).append("=").append(v).append(",");
  }
  newIndex.back() = '}';

  auto it = sm.nameMap_.find(newIndex);
  // Get the counter if it already exists
  if (it != sm.nameMap_.end()) {
    return it->second.id_;
  }

  // Register a new counter if it doesn't exist
  auto it2 = sm.nameMap_.find(index);
  DCHECK(it2 != sm.nameMap_.end());
  auto& methods = it2->second.methods_;

  return registerStats(newIndex, methods);
}

// static
CounterId StatsManager::histoWithLabels(const CounterId& id, const std::vector<LabelPair>& labels) {
  using std::chrono::seconds;

  auto& sm = get();
  auto index = id.index();
  CHECK(!labels.empty());
  std::string newIndex;
  newIndex.append(index);
  newIndex.append("{");
  for (auto& [k, v] : labels) {
    newIndex.append(k).append("=").append(v).append(",");
  }
  newIndex.back() = '}';
  auto it = sm.nameMap_.find(newIndex);
  // Get the counter if it already exists
  if (it != sm.nameMap_.end()) {
    return it->second.id_;
  }

  auto it2 = sm.nameMap_.find(index);
  DCHECK(it2 != sm.nameMap_.end());
  auto& methods = it2->second.methods_;
  auto& percentiles = it2->second.percentiles_;

  return registerHisto(
      newIndex, it2->second.bucketSize_, it2->second.min_, it2->second.max_, methods, percentiles);
}

// static
void StatsManager::removeCounterWithLabels(const CounterId& id,
                                           const std::vector<LabelPair>& labels) {
  using std::chrono::seconds;

  auto& sm = get();
  auto index = id.index();
  CHECK(!labels.empty());
  std::string newIndex;
  newIndex.append(index);
  newIndex.append("{");
  for (auto& [k, v] : labels) {
    newIndex.append(k).append("=").append(v).append(",");
  }
  newIndex.back() = '}';
  folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
  auto it = sm.nameMap_.find(newIndex);
  if (it != sm.nameMap_.end()) {
    sm.nameMap_.erase(it);
  }
  sm.stats_.erase(newIndex);
}

// static
void StatsManager::removeHistoWithLabels(const CounterId& id,
                                         const std::vector<LabelPair>& labels) {
  using std::chrono::seconds;

  auto& sm = get();
  auto index = id.index();
  CHECK(!labels.empty());
  std::string newIndex;
  newIndex.append(index);
  newIndex.append("{");
  for (auto& [k, v] : labels) {
    newIndex.append(k).append("=").append(v).append(",");
  }
  newIndex.back() = '}';
  folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
  auto it = sm.nameMap_.find(newIndex);
  if (it != sm.nameMap_.end()) {
    sm.nameMap_.erase(it);
  }
  sm.histograms_.erase(newIndex);
}

// static
void StatsManager::addValue(const CounterId& id, VT value) {
  using std::chrono::seconds;

  auto& sm = get();
  if (!id.valid()) {
    // The counter is not registered
    return;
  }
  std::string index = id.index();
  bool isHisto = id.isHisto();
  if (!isHisto) {
    // Stats
    auto iter = sm.stats_.find(index);
    if (iter != sm.stats_.end()) {
      std::lock_guard<std::mutex> g(*(iter->second.first));
      iter->second.second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    }
  } else {
    // Histogram
    auto iter = sm.histograms_.find(index);
    if (iter != sm.histograms_.end()) {
      std::lock_guard<std::mutex> g(*(iter->second.first));
      iter->second.second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    }
  }
}

// static
void StatsManager::decValue(const CounterId& id, VT value) {
  addValue(id, -value);
}

// static
bool StatsManager::strToPct(folly::StringPiece part, double& pct) {
  static const int32_t divisors[] = {1, 1, 10, 100, 1000, 10000};
  try {
    size_t len = part.size() - 1;
    if (len > 0 && len <= 6) {
      auto digits = folly::StringPiece(&(part[1]), len);
      pct = folly::to<double>(digits) / divisors[len - 1];
      return true;
    } else {
      LOG(ERROR) << "Precision " << part.toString() << " is too long";
      return false;
    }
  } catch (const std::exception& ex) {
    LOG(ERROR) << "Failed to convert the digits to a double: " << ex.what();
  }

  return false;
}

// static
StatusOr<StatsManager::VT> StatsManager::readValue(folly::StringPiece metricName) {
  std::vector<std::string> parts;
  folly::split(".", metricName, parts, true);
  if (parts.size() != 3) {
    LOG(ERROR) << "\"" << metricName << "\" is not a valid metric name";
    return Status::Error("\"%s\" is not a valid metric name", metricName.data());
  }

  TimeRange range;
  if (parts[2] == "5") {
    range = TimeRange::FIVE_SECONDS;
  } else if (parts[2] == "60") {
    range = TimeRange::ONE_MINUTE;
  } else if (parts[2] == "600") {
    range = TimeRange::TEN_MINUTES;
  } else if (parts[2] == "3600") {
    range = TimeRange::ONE_HOUR;
  } else {
    // Unsupported time range
    LOG(ERROR) << "Unsupported time range \"" << parts[2] << "\"";
    return Status::Error(folly::stringPrintf("Unsupported time range \"%s\"", parts[2].c_str()));
  }

  // Now check the statistic method
  folly::toLowerAscii(parts[1]);
  if (parts[1] == "sum") {
    return readStats(parts[0], range, StatsMethod::SUM);
  } else if (parts[1] == "count") {
    return readStats(parts[0], range, StatsMethod::COUNT);
  } else if (parts[1] == "avg") {
    return readStats(parts[0], range, StatsMethod::AVG);
  } else if (parts[1] == "rate") {
    return readStats(parts[0], range, StatsMethod::RATE);
  } else if (parts[1][0] == 'p') {
    // Percentile
    double pct;
    if (strToPct(parts[1], pct)) {
      return readHisto(parts[0], range, pct);
    } else {
      LOG(ERROR) << "\"" << parts[1] << "\" is not a valid percentile form";
      return Status::Error(
          folly::stringPrintf("\"%s\" is not a valid percentile form", parts[1].c_str()));
    }
  } else {
    LOG(ERROR) << "Unsupported statistic method \"" << parts[1] << "\"";
    return Status::Error(
        folly::stringPrintf("Unsupported statistic method \"%s\"", parts[1].c_str()));
  }
}

// static
void StatsManager::readAllValue(folly::dynamic& vals) {
  auto& sm = get();

  for (auto const& statsName : sm.nameMap_) {
    // Add stats
    for (auto const& method : statsName.second.methods_) {
      std::string metricPrefix = statsName.first;
      switch (method) {
        case StatsMethod::SUM:
          metricPrefix += ".sum";
          break;
        case StatsMethod::COUNT:
          metricPrefix += ".count";
          break;
        case StatsMethod::AVG:
          metricPrefix += ".avg";
          break;
        case StatsMethod::RATE:
          metricPrefix += ".rate";
          break;
          // intentionally no `default'
      }

      for (auto range = TimeRange::FIVE_SECONDS; range <= TimeRange::ONE_HOUR;
           range = static_cast<TimeRange>(static_cast<int>(range) + 1)) {
        std::string metricName;
        switch (range) {
          case TimeRange::FIVE_SECONDS:
            metricName = metricPrefix + ".5";
            break;
          case TimeRange::ONE_MINUTE:
            metricName = metricPrefix + ".60";
            break;
          case TimeRange::TEN_MINUTES:
            metricName = metricPrefix + ".600";
            break;
          case TimeRange::ONE_HOUR:
            metricName = metricPrefix + ".3600";
            break;
            // intentionally no `default'
        }

        auto status = readStats(statsName.second.id_, range, method);
        CHECK(status.ok());
        int64_t metricValue = status.value();
        vals.push_back(folly::dynamic::object(metricName, metricValue));
      }
    }

    // add Percentile
    for (auto& pct : statsName.second.percentiles_) {
      auto metricPrefix = statsName.first + "." + pct.first;
      for (auto range = TimeRange::FIVE_SECONDS; range <= TimeRange::ONE_HOUR;
           range = static_cast<TimeRange>(static_cast<int>(range) + 1)) {
        std::string metricName;
        switch (range) {
          case TimeRange::FIVE_SECONDS:
            metricName = metricPrefix + ".5";
            break;
          case TimeRange::ONE_MINUTE:
            metricName = metricPrefix + ".60";
            break;
          case TimeRange::TEN_MINUTES:
            metricName = metricPrefix + ".600";
            break;
          case TimeRange::ONE_HOUR:
            metricName = metricPrefix + ".3600";
            break;
            // intentionally no `default'
        }

        auto status = readHisto(statsName.second.id_, range, pct.second);
        if (!status.ok()) {
          LOG(ERROR) << "Failed to read histogram " << metricName;
          continue;
        }
        auto metricValue = status.value();
        vals.push_back(folly::dynamic::object(metricName, metricValue));
      }
    }
  }
}

// static
StatusOr<StatsManager::VT> StatsManager::readStats(const CounterId& id,
                                                   StatsManager::TimeRange range,
                                                   StatsManager::StatsMethod method) {
  using std::chrono::seconds;
  auto& sm = get();
  std::string index = id.index();

  if (index == "") {
    return Status::Error("Invalid stats");
  }

  if (!id.isHisto()) {
    // stats
    auto iter = sm.stats_.find(index);
    DCHECK(iter != sm.stats_.end());
    std::lock_guard<std::mutex> g(*(iter->second.first));
    iter->second.second->update(seconds(time::WallClock::fastNowInSec()));
    return readValue(*(iter->second.second), range, method);
  } else {
    // histograms_
    auto iter = sm.histograms_.find(index);
    DCHECK(iter != sm.histograms_.end());
    std::lock_guard<std::mutex> g(*(iter->second.first));
    iter->second.second->update(seconds(time::WallClock::fastNowInSec()));
    return readValue(*(iter->second.second), range, method);
  }
}

// static
StatusOr<StatsManager::VT> StatsManager::readStats(const std::string& counterName,
                                                   StatsManager::TimeRange range,
                                                   StatsManager::StatsMethod method) {
  auto& sm = get();

  // Look up the counter name
  CounterId const* id;
  {
    folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
    auto it = sm.nameMap_.find(counterName);
    if (it == sm.nameMap_.end()) {
      // Not found
      return Status::Error("Stats not found \"%s\"", counterName.c_str());
    }

    id = &(it->second.id_);
  }

  return readStats(*id, range, method);
}

// static
StatusOr<StatsManager::VT> StatsManager::readHisto(const CounterId& id,
                                                   StatsManager::TimeRange range,
                                                   double pct) {
  using std::chrono::seconds;
  auto& sm = get();
  std::string index = id.index();

  if (!id.isHisto()) {
    return Status::Error("Invalid stats");
  }

  auto it = sm.histograms_.find(index);
  if (it == sm.histograms_.end()) {
    return Status::Error("Invalid stats");
  }

  std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
  sm.histograms_[index].second->update(seconds(time::WallClock::fastNowInSec()));
  auto level = static_cast<size_t>(range);
  return sm.histograms_[index].second->getPercentileEstimate(pct, level);
}

// static
StatusOr<StatsManager::VT> StatsManager::readHisto(const std::string& counterName,
                                                   StatsManager::TimeRange range,
                                                   double pct) {
  auto& sm = get();

  // Look up the counter name
  CounterId const* id;
  {
    folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
    auto it = sm.nameMap_.find(counterName);
    if (it == sm.nameMap_.end()) {
      // Not found
      return Status::Error("Stats not found \"%s\"", counterName.c_str());
    }

    id = &(it->second.id_);
  }

  return readHisto(*id, range, pct);
}

}  // namespace stats
}  // namespace nebula
