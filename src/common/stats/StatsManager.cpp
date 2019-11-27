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

    std::string name = counterName.toString();
    auto it = sm.nameMap_.find(name);
    if (it != sm.nameMap_.end()) {
        LOG(INFO) << "The counter \"" << name << "\" already exists";
        return it->second;
    }

    // Insert the Stats
    sm.stats_.emplace_back(
        std::make_pair(
            std::make_unique<std::mutex>(),
            std::make_unique<StatsType>(
                60,
                std::initializer_list<StatsType::Duration>({seconds(60),
                                                            seconds(600),
                                                            seconds(3600)}))));
    int32_t index = sm.stats_.size();
    sm.nameMap_[name] = index;
    return index;
}


// static
int32_t StatsManager::registerHisto(folly::StringPiece counterName,
                                    StatsManager::VT bucketSize,
                                    StatsManager::VT min,
                                    StatsManager::VT max) {
    using std::chrono::seconds;

    auto& sm = get();
    std::string name = counterName.toString();
    auto it = sm.nameMap_.find(name);
    if (it != sm.nameMap_.end()) {
        LOG(ERROR) << "The counter \"" << name << "\" already exists";
        return it->second;
    }

    // Insert the Histogram
    sm.histograms_.emplace_back(
        std::make_pair(
            std::make_unique<std::mutex>(),
            std::make_unique<HistogramType>(
                bucketSize,
                min,
                max,
                StatsType(60, {seconds(60), seconds(600), seconds(3600)}))));
    int32_t index = - sm.histograms_.size();
    sm.nameMap_[name] = index;
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
        DCHECK_LT(index, sm.stats_.size());
        std::lock_guard<std::mutex> g(*(sm.stats_[index].first));
        sm.stats_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    } else {
        // Histogram
        index = - (index + 1);
        DCHECK_LT(index, sm.histograms_.size());
        std::lock_guard<std::mutex> g(*(sm.histograms_[index].first));
        sm.histograms_[index].second->addValue(seconds(time::WallClock::fastNowInSec()), value);
    }
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
    if (parts[2] == "60") {
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
        return Status::Error(folly::stringPrintf("\"%s\" is not a valid percentile form",
                                                 parts[1].c_str()));
    } else {
        LOG(ERROR) << "Unsupported statistic method \"" << parts[1] << "\"";
        return Status::Error(folly::stringPrintf("Unsupported statistic method \"%s\"",
                                                 parts[1].c_str()));
    }
}


// static
void StatsManager::readAllValue(folly::dynamic& vals) {
    auto& sm = get();

    for (auto &statsName : sm.nameMap_) {
        for (auto method = StatsMethod::SUM; method <= StatsMethod::RATE;
             method = static_cast<StatsMethod>(static_cast<int>(method) + 1)) {
            for (auto range = TimeRange::ONE_MINUTE; range <= TimeRange::ONE_HOUR;
                 range = static_cast<TimeRange>(static_cast<int>(range) + 1)) {
                std::string metricName = statsName.first;
                auto status = readStats(statsName.second, range, method);
                CHECK(status.ok());
                int64_t metricValue = status.value();
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
StatusOr<StatsManager::VT> StatsManager::readStats(int32_t index,
                                         StatsManager::TimeRange range,
                                         StatsManager::StatsMethod method) {
    using std::chrono::seconds;
    auto& sm = get();

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
    int32_t index = 0;

    {
        auto it = sm.nameMap_.find(counterName);
        if (it == sm.nameMap_.end()) {
            // Not found
            return Status::Error("Stats not found \"%s\"", counterName.c_str());
        }

        index = it->second;
    }

    return readStats(index, range, method);
}


// static
StatusOr<StatsManager::VT> StatsManager::readHisto(const std::string& counterName,
                                                   StatsManager::TimeRange range,
                                                   double pct) {
    using std::chrono::seconds;
    auto& sm = get();

    // Look up the counter name
    int32_t index = 0;
    {
        auto it = sm.nameMap_.find(counterName);
        if (it == sm.nameMap_.end()) {
            // Not found
            return Status::Error("Stats not found \"%s\"", counterName.c_str());
        }

        index = it->second;
    }

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
constexpr bool StatsManager::isStatIndex(int32_t i) {
    return i > 0;
}

// static
constexpr bool StatsManager::isHistoIndex(int32_t i) {
    return i < 0;
}

// static
constexpr std::size_t StatsManager::physicalStatIndex(int32_t i) {
    DCHECK(isStatIndex(i));
    return i - 1;
}

// static
constexpr std::size_t StatsManager::physicalHistoIndex(int32_t i) {
    DCHECK(isHistoIndex(i));
    return -(i + 1);
}

// static
StatusOr<StatsManager::ParsedName>
StatsManager::parseMetricName(folly::StringPiece metricName) {
    std::string name;
    StatsMethod method;
    TimeRange range;
    std::vector<std::string> parts;
    folly::split(".", metricName, parts, true);
    if (parts.size() != 3) {
        LOG(ERROR) << "\"" << metricName << "\" is not a valid metric name";
        return Status::Error("\"%s\" is not a valid metric name", metricName.data());
    }

    name = parts[0];

    if (parts[2] == "60") {
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

    folly::toLowerAscii(parts[1]);
    if (parts[1] == "sum") {
        method = StatsMethod::SUM;
    } else if (parts[1] == "count") {
        method = StatsMethod::COUNT;
    } else if (parts[1] == "avg") {
        method = StatsMethod::AVG;
    } else if (parts[1] == "rate") {
        method = StatsMethod::RATE;
    // } else if (parts[1][0] == 'p') {   // TODO(shylock)
    } else {
        LOG(ERROR) << "Unsupported statistic method \"" << parts[1] << "\"";
        return Status::Error(folly::stringPrintf("Unsupported statistic method \"%s\"",
                                                 parts[1].c_str()));
    }

    return StatsManager::ParsedName {
        name,
        method,
        range,
    };
}


void PrometheusSerializer::annotate(
        std::ostream& out, const std::string metricName, const std::string metricType) const {
    out << "# HELP " << metricName << " Record all " << metricType << " about nebula" << "\n";
    out << "# TYPE " << metricName << " " << metricType << "\n";
}

// Write a double as a string, with proper formatting for infinity and NaN
void PrometheusSerializer::writeValue(std::ostream& out, double value) const {
    if (std::isnan(value)) {
        out << "Nan";
    } else if (std::isinf(value)) {
        out << (value < 0 ? "-Inf" : "+Inf");
    } else {
        auto saved_flags = out.setf(std::ios::fixed, std::ios::floatfield);
        out << value;
        out.setf(saved_flags, std::ios::floatfield);
    }
}

void PrometheusSerializer::writeValue(std::ostream& out, const int64_t value) const {
    if (std::isnan(value)) {
        out << "Nan";
    } else if (std::isinf(value)) {
        out << (value < 0 ? "-Inf" : "+Inf");
    } else {
        out << value;
    }
}

void PrometheusSerializer::writeValue(std::ostream& out, const std::string& value) const {
    for (auto c : value) {
        switch (c) {
        case '\n':
            out << '\\' << 'n';
            break;

        case '\\':
            out << '\\' << c;
            break;

        case '"':
            out << '\\' << c;
            break;

        default:
            out << c;
            break;
        }
    }
}

template<typename T>
void PrometheusSerializer::writeHead(std::ostream& out, const std::string& family,
        const std::vector<Label>& labels, const std::string& suffix,
        const std::string& extraLabelName,
        const T extraLabelValue) const {
    out << family << suffix;
    if (!labels.empty() || !extraLabelName.empty()) {
        out << "{";
        const char* prefix = "";

        for (auto& lp : labels) {
            out << prefix << lp.name << "=\"";
            writeValue(out, lp.value);
            out << "\"";
            prefix = ",";
        }
        if (!extraLabelName.empty()) {
            out << prefix << extraLabelName << "=\"";
            writeValue(out, extraLabelValue);
            out << "\"";
        }
        out << "}";
    }
    out << " ";
}

// Write a line trailer: timestamp
void PrometheusSerializer::writeTail(std::ostream& out, const std::int64_t timestampMs) const {
    if (timestampMs != 0) {
        out << " " << timestampMs;
    }
    out << "\n";
}

// TODO(shylock) transform nebula stats to Prometheus::ClientMetric/MetricFamily
// using the serrializing from 3rd-SDK
void StatsManager::prometheusSerialize(std::ostream& out) /*const*/ {
    std::locale saved_locale = out.getloc();
    out.imbue(std::locale::classic());
    for (auto& index : nameMap_) {
        auto parsedName = parseMetricName(index.first);
        std::string name = parsedName.ok() ? parsedName.value().name : index.first;
        if (isStatIndex(index.second)) {
            annotate(out, name, "gauge");

            writeHead(out, name, {});
            if (parsedName.ok()) {
                // make sure contains value
                writeValue(out,
                    readStats(index.second, TimeRange::ONE_MINUTE, StatsMethod::AVG).value());
            } else {
                // make sure contains value
                writeValue(out,
                    readStats(index.second, TimeRange::ONE_MINUTE, StatsMethod::AVG).value());
            }
            writeTail(out);
        } else if (isHistoIndex(index.second)) {
            annotate(out, name, "histogram");

            writeHead(out, name, {}, "_count");
            if (parsedName.ok()) {
                out <<
                    readStats(index.second, parsedName.value().range, StatsMethod::COUNT).value();
            } else {
                out << readStats(index.second, TimeRange::ONE_HOUR, StatsMethod::COUNT).value();
            }
            writeTail(out);

            writeHead(out, name, {}, "_sum");
            if (parsedName.ok()) {
                writeValue(out,
                    readStats(index.second, parsedName.value().range, StatsMethod::SUM).value());
            } else {
                writeValue(out,
                    readStats(index.second, TimeRange::ONE_HOUR, StatsMethod::SUM).value());
            }
            writeTail(out);

            auto& p = histograms_[physicalHistoIndex(index.second)];
            std::lock_guard<std::mutex> lk(*p.first);
            auto& hist = p.second;
            VT last = hist->getMax();
            VT cumulativeCount = 0;
            auto diff = (hist->getMax() - hist->getMin()) /
                static_cast<double>(hist->getNumBuckets());
            double bound = hist->getMin() + diff;
            for (std::size_t i = 0; i < hist->getNumBuckets(); ++i) {
                writeHead(out, name, {}, "_bucket", "le", bound);
                if (parsedName.ok()) {
                    cumulativeCount +=hist->getBucket(i)
                        .count(static_cast<std::size_t>(parsedName.value().range));
                } else {
                    cumulativeCount += hist->getBucket(i)
                        .count(static_cast<std::size_t>(TimeRange::ONE_HOUR));
                }
                out << cumulativeCount;
                writeTail(out);
                bound += diff;
            }

            if (!std::isinf(last)) {
                writeHead(out, name, {}, "_bucket", "le", "+Inf");
                out << cumulativeCount;
                writeTail(out);
            }
        } else {
            DCHECK(false) << "Impossible neither stat nor histogram!";
        }
    }

    out.imbue(saved_locale);
}

}  // namespace stats
}  // namespace nebula

