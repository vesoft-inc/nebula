/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "stats/StatsManager.h"
#include <folly/stats/MultiLevelTimeSeries-defs.h>
#include <folly/stats/TimeseriesHistogram-defs.h>

namespace nebula {
namespace stats {

// static
StatsManager& StatsManager::get() {
    static StatsManager sm;
    return sm;
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
int32_t StatsManager::registerStats(folly::StringPiece counterName) {
    using std::chrono::seconds;

    auto& sm = get();

    std::string name;
    // Insert the name first
    {
        folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
        name = counterName.toString();
        auto it = sm.nameMap_.find(name);
        if (it != sm.nameMap_.end()) {
            // Found
            LOG(ERROR) << "The counter \"" << name << "\" already exists";
        }

        sm.nameMap_.emplace(name, 0);
    }

    // Insert the Stats
    int32_t index = 0;
    {
        folly::RWSpinLock::WriteHolder wh(sm.statsLock_);
        sm.stats_.emplace_back(
            std::make_pair(
                std::make_unique<std::mutex>(),
                std::make_unique<StatsType>(
                    60,
                    std::initializer_list<StatsType::Duration>({seconds(60),
                                                                seconds(600),
                                                                seconds(3600)}))));
        index = sm.stats_.size();
    }

    // Update the index
    {
        folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
        sm.nameMap_[name] = index;
    }

    return index;
}


// static
int32_t StatsManager::registerHisto(folly::StringPiece counterName,
                                    StatsManager::VT bucketSize,
                                    StatsManager::VT min,
                                    StatsManager::VT max) {
    using std::chrono::seconds;

    auto& sm = get();

    std::string name;
    // Insert the name first
    {
        folly::RWSpinLock::WriteHolder wh(sm.nameMapLock_);
        name = counterName.toString();
        auto it = sm.nameMap_.find(name);
        if (it != sm.nameMap_.end()) {
            // Found
            LOG(ERROR) << "The counter \"" << name << "\" already exists";
        }

        sm.nameMap_.emplace(name, 0);
    }

    // Insert the Histogram
    int32_t index = 0;
    {
        folly::RWSpinLock::WriteHolder wh(sm.histogramsLock_);
        sm.histograms_.emplace_back(
            std::make_pair(
                std::make_unique<std::mutex>(),
                std::make_unique<HistogramType>(
                    bucketSize,
                    min,
                    max,
                    StatsType(60, {seconds(60), seconds(600), seconds(3600)}))));
        index = - sm.histograms_.size();
    }

    // Update the index
    {
        folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
        sm.nameMap_[name] = index;
    }

    return index;
}


// static
void StatsManager::addValue(int32_t index, VT value) {
    using std::chrono::seconds;
    CHECK_NE(index, 0);

    auto& sm = get();
    if (index > 0) {
        // Stats
        --index;
        folly::RWSpinLock::ReadHolder rh(sm.statsLock_);
        DCHECK_LT(index, sm.stats_.size());
        std::lock_guard<std::mutex> g(*(sm.stats_[index].first));
        sm.stats_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    } else {
        // Histogram
        index = - (index + 1);
        folly::RWSpinLock::ReadHolder rh(sm.histogramsLock_);
        DCHECK_LT(index, sm.histograms_.size());
        std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
        sm.histograms_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    }
}


// static
StatsManager::VT StatsManager::readValue(folly::StringPiece metricName) {
    std::vector<std::string> parts;
    folly::split(".", metricName, parts, true);
    if (parts.size() != 3) {
        LOG(ERROR) << "\"" << metricName << "\" is not a valid metric name";
        return 0;
    }

    TimeRange range;
    if (parts[2] == "60") {
        range = TimeRange::ONE_MINUTE;
    } else if (parts[2] == "600") {
        range = TimeRange::TEN_MINUTES;
    } else if (parts[2] == "3600") {
        range = TimeRange::ONE_HOUR;
    } else {
        // Unsupported time range
        LOG(ERROR) << "Unsupported time range \"" << parts[2] << "\"";
        return 0;
    }

    // Now check the statistic method
    static int32_t dividors[] = {1, 1, 10, 100, 1000, 10000};
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
        try {
            size_t len = parts[1].size()  - 1;
            if (len > 0 && len <= 6) {
                auto digits = folly::StringPiece(&(parts[1][1]), len);
                auto pct = folly::to<double>(digits) / dividors[len - 1];
                return readHisto(parts[0], range, pct);
            }
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Failed to convert the digits to a double: " << ex.what();
        }

        LOG(ERROR) << "\"" << parts[1] << "\" is not a valid percentile form";
        return 0;
    } else {
        LOG(ERROR) << "Unsupported statistic method \"" << parts[1] << "\"";
        return 0;
    }
}


// static
void StatsManager::readAllValue(folly::dynamic& vals) {
    auto& sm = get();

    folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);

    for (auto &statsName : sm.nameMap_) {
        for (auto method = StatsMethod::SUM; method <= StatsMethod::RATE;
             method = static_cast<StatsMethod>(static_cast<int>(method) + 1)) {
            for (auto range = TimeRange::ONE_MINUTE; range <= TimeRange::ONE_HOUR;
                 range = static_cast<TimeRange>(static_cast<int>(range) + 1)) {
                std::string metricName = statsName.first;
                int64_t metricValue = readStats(statsName.second, range, method);
                folly::dynamic stat = folly::dynamic::object();

                switch (method) {
                    case StatsMethod::SUM:
                        metricName += ".sum";
                        break;
                    case StatsMethod::COUNT:
                        metricName += ".count";
                        break;
                    case StatsMethod::AVG:
                        metricName += ".avg";
                        break;
                   case StatsMethod::RATE:
                        metricName += ".rate";
                        break;
                    // intentionally no `default'
                }

                switch (range) {
                    case TimeRange::ONE_MINUTE:
                        metricName += ".60";
                        break;
                    case TimeRange::TEN_MINUTES:
                        metricName += ".600";
                        break;
                    case TimeRange::ONE_HOUR:
                        metricName += ".3600";
                        break;
                    // intentionally no `default'
                }

                stat["name"] = metricName;
                stat["value"] = metricValue;
                vals.push_back(std::move(stat));
            }
        }
    }
}


// static
StatsManager::VT StatsManager::readStats(int32_t index,
                                         StatsManager::TimeRange range,
                                         StatsManager::StatsMethod method) {
    using std::chrono::seconds;
    auto& sm = get();


    CHECK_NE(index, 0);

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
StatsManager::VT StatsManager::readStats(const std::string& counterName,
                                         StatsManager::TimeRange range,
                                         StatsManager::StatsMethod method) {
    auto& sm = get();

    // Look up the counter name
    int32_t index = 0;

    {
        folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
        auto it = sm.nameMap_.find(counterName);
        if (it == sm.nameMap_.end()) {
            // Not found
            return 0;
        }

        index = it->second;
    }

    return readStats(index, range, method);
}


// static
StatsManager::VT StatsManager::readHisto(const std::string& counterName,
                                         StatsManager::TimeRange range,
                                         double pct) {
    using std::chrono::seconds;
    auto& sm = get();

    // Look up the counter name
    int32_t index = 0;
    {
        folly::RWSpinLock::ReadHolder rh(sm.nameMapLock_);
        auto it = sm.nameMap_.find(counterName);
        if (it == sm.nameMap_.end()) {
            // Not found
            return 0;
        }

        index = it->second;
    }

    CHECK_LT(index, 0);
    index = - (index + 1);
    DCHECK_LT(index, sm.histograms_.size());

    std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
    sm.histograms_[index].second->update(seconds(time::WallClock::fastNowInSec()));
    auto level = static_cast<size_t>(range);
    return sm.histograms_[index].second->getPercentileEstimate(pct, level);
}

}  // namespace stats
}  // namespace nebula

