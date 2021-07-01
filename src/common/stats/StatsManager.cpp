/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include <folly/String.h>

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
                percentiles.push_back(std::make_pair(trimmedPart, pct));
            } else {
                LOG(ERROR) << "\"" << trimmedPart << "\" is not a valid percentile form";
            }
        } else {
            LOG(ERROR) << "Unsupported statistic method \"" << trimmedPart << "\"";
        }
    }
}


// static
CounterId StatsManager::registerStats(folly::StringPiece counterName,
                                      std::string stats) {
    using std::chrono::seconds;

    auto& sm = get();
    std::vector<StatsMethod> methods;
    std::vector<std::pair<std::string, double>> percentiles;
    parseStats(stats, methods, percentiles);

    std::string name = counterName.toString();
    folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
    auto it = sm.nameMap_.find(name);
    if (it != sm.nameMap_.end()) {
        DCHECK_GT(it->second.id_.index(), 0);
        VLOG(2) << "The counter \"" << name << "\" already exists";
        it->second.methods_ = methods;
        return it->second.id_.index();
    }

    // Insert the Stats
    sm.stats_.emplace_back(
        std::make_pair(
            std::make_unique<std::mutex>(),
            std::make_unique<StatsType>(
                60,
                std::initializer_list<StatsType::Duration>({seconds(5),
                                                            seconds(60),
                                                            seconds(600),
                                                            seconds(3600)}))));
    int32_t index = sm.stats_.size();
    sm.nameMap_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(std::move(name)),
        std::forward_as_tuple(index,
                              std::move(methods),
                              std::vector<std::pair<std::string, double>>()));

    VLOG(1) << "Registered stats " << counterName.toString();
    return index;
}


// static
CounterId StatsManager::registerHisto(folly::StringPiece counterName,
                                      StatsManager::VT bucketSize,
                                      StatsManager::VT min,
                                      StatsManager::VT max,
                                      std::string stats) {
    using std::chrono::seconds;
    std::vector<StatsMethod> methods;
    std::vector<std::pair<std::string, double>> percentiles;
    parseStats(stats, methods, percentiles);

    auto& sm = get();
    std::string name = counterName.toString();
    folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
    auto it = sm.nameMap_.find(name);
    if (it != sm.nameMap_.end()) {
        DCHECK_LT(it->second.id_.index(), 0);
        VLOG(2) << "The counter \"" << name << "\" already exists";
        it->second.methods_ = methods;
        it->second.percentiles_ = percentiles;
        return it->second.id_.index();
    }

    // Insert the Histogram
    sm.histograms_.emplace_back(
        std::make_pair(
            std::make_unique<std::mutex>(),
            std::make_unique<HistogramType>(
                bucketSize,
                min,
                max,
                StatsType(60, {seconds(5), seconds(60), seconds(600), seconds(3600)}))));
    int32_t index = - sm.histograms_.size();
    sm.nameMap_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(std::move(name)),
        std::forward_as_tuple(index,
                              std::move(methods),
                              std::move(percentiles)));

    VLOG(1) << "Registered histogram " << counterName.toString()
            << " [bucketSize: " << bucketSize
            << ", min value: " << min
            << ", max value: " << max
            << "]";
    return index;
}


// static
void StatsManager::addValue(const CounterId& id, VT value) {
    using std::chrono::seconds;

    auto& sm = get();
    int32_t index = id.index();
    if (index > 0) {
        // Stats
        --index;
        DCHECK_LT(index, sm.stats_.size());
        std::lock_guard<std::mutex> g(*(sm.stats_[index].first));
        sm.stats_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    } else if (index < 0) {
        // Histogram
        index = - (index + 1);
        DCHECK_LT(index, sm.histograms_.size());
        std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
        sm.histograms_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    } else {
        LOG(FATAL) << "Invalid counter id";
    }
}


// static
bool StatsManager::strToPct(folly::StringPiece part, double& pct) {
    static const int32_t dividors[] = {1, 1, 10, 100, 1000, 10000};
    try {
        size_t len = part.size()  - 1;
        if (len > 0 && len <= 6) {
            auto digits = folly::StringPiece(&(part[1]), len);
            pct = folly::to<double>(digits) / dividors[len - 1];
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
        return Status::Error(folly::stringPrintf("Unsupported time range \"%s\"",
                                                 parts[2].c_str()));
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
            return Status::Error(folly::stringPrintf("\"%s\" is not a valid percentile form",
                                                     parts[1].c_str()));
        }
    } else {
        LOG(ERROR) << "Unsupported statistic method \"" << parts[1] << "\"";
        return Status::Error(folly::stringPrintf("Unsupported statistic method \"%s\"",
                                                 parts[1].c_str()));
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

            for (auto range = TimeRange::FIVE_SECONDS;
                 range <= TimeRange::ONE_HOUR;
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
            for (auto range = TimeRange::FIVE_SECONDS;
                 range <= TimeRange::ONE_HOUR;
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
    int32_t index = id.index();

    if (index == 0) {
        return Status::Error("Invalid stats");
    }

    if (index > 0) {
        // stats
        --index;
        DCHECK_LT(index, sm.stats_.size());
        std::lock_guard<std::mutex> g(*(sm.stats_[index].first));
        sm.stats_[index].second->update(seconds(time::WallClock::fastNowInSec()));
        return readValue(*(sm.stats_[index].second), range, method);
    } else {
        // histograms_
        index = - (index + 1);
        DCHECK_LT(index, sm.histograms_.size());
        std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
        sm.histograms_[index].second->update(seconds(time::WallClock::fastNowInSec()));
        return readValue(*(sm.histograms_[index].second), range, method);
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
    int32_t index = id.index();

    if (index >= 0) {
        return Status::Error("Invalid stats");
    }
    index = - (index + 1);
    if (static_cast<size_t>(index) >= sm.histograms_.size()) {
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
