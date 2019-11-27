/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_STATS_STATSMANAGER_H_
#define COMMON_STATS_STATSMANAGER_H_

#include "base/Base.h"
#include "time/WallClock.h"
#include "base/StatusOr.h"
#include <folly/RWSpinLock.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/stats/TimeseriesHistogram.h>

namespace nebula {
namespace stats {

class MetricsSerializer {
public:
    virtual ~MetricsSerializer() = default;
    std::string serialize() /*const*/ {
        std::ostringstream ss;
        serialize(ss);
        return ss.str();
    }
    virtual void serialize(std::ostream& out) /*const*/ = 0;
};

class PrometheusSerializer : public MetricsSerializer {
public:
    virtual ~PrometheusSerializer() = default;
    void serialize(std::ostream& out) /*const*/ override {
        prometheusSerialize(out);
    }
    virtual void prometheusSerialize(std::ostream& out) /*const*/ = 0;
    struct Label {
        std::string name;
        std::string value;
    };
    void annotate(std::ostream& out, const std::string metricName,
        const std::string metricType) const;
    void writeValue(std::ostream& out, double value) const;
    void writeValue(std::ostream& out, const int64_t value) const;
    void writeValue(std::ostream& out, const std::string& value) const;
    template <typename T = std::string>
    void writeHead(std::ostream& out, const std::string& family,
        const std::vector<Label>& labels, const std::string& suffix = "",
        const std::string& extraLabelName = "",
        const T extraLabelValue = T()) const;
    void writeTail(std::ostream& out, const std::int64_t timestampMs = 0) const;
};

/**
 * This is a utility class to keep track the service's statistic information.
 *
 * It contains a bunch of counters. Each counter has a unique name and three
 * levels of time ranges, which are 1 minute, 10 minues, and 1 hour. Each
 * counter also associates with one or multiple statistic types. The supported
 * statistic types are SUM, COUNT, AVG, RATE, MIN, MAX and percentiles. Among
 * those, MIN, MAX and Percentile are only supported for Histogram counters.
 * The percentile is in the form of "pXXXXX". There could be 2 or more digits
 * after the letter "p", which represents XX.XXXX%.
 *
 * Here is the complete form of a specific counter:
 *   <counter_name>.<statistic_type>.<time_range>
 *
 * Here are some examples:
 *   query.rate.60      -- query per second in the last minute
 *   latency.p99.600    -- The latency that slower than 99% of all queries
 *                           in the last ten minutes
 *   latency.p9999.60   -- The latency that slower than 99.99% of all queries
 *                           in the last one minute
 *   error.count.600    -- Total number of errors in the last ten minutes
 */
class StatsManager final : public PrometheusSerializer {
    using VT = int64_t;
    using StatsType = folly::MultiLevelTimeSeries<VT>;
    using HistogramType = folly::TimeseriesHistogram<VT>;

public:
    enum class StatsMethod {
        SUM = 1,
        COUNT,
        AVG,
        RATE
    };

    enum class TimeRange {
        ONE_MINUTE = 0,
        TEN_MINUTES = 1,
        ONE_HOUR = 2
    };

    struct ParsedName {
        std::string name;
        StatsMethod method;
        TimeRange range;
    };

    static void setDomain(folly::StringPiece domain);
    // addr     -- The ip/port of the stats collector. StatsManager will periodically
    //             report the stats to the collector
    // interval -- The number of seconds between each report
    static void setReportInfo(HostAddr addr, int32_t interval);

    // Both register methods return the index to the internal data structure.
    // This index will be used by addValue() methods.
    // Both register methods is not thread safe, and user need to register stats one
    // by onebefore calling addValue, readStats, readHisto and so on.
    static int32_t registerStats(folly::StringPiece counterName);
    static int32_t registerHisto(folly::StringPiece counterName,
                                 VT bucketSize,
                                 VT min,
                                 VT max);

    static void addValue(int32_t index, VT value = 1);

    static StatusOr<VT> readValue(folly::StringPiece counter);
    static StatusOr<VT> readStats(int32_t index,
                                  TimeRange range,
                                  StatsMethod method);
    static StatusOr<VT> readStats(const std::string& counterName,
                                  TimeRange range,
                                  StatsMethod method);
    static StatusOr<VT> readHisto(const std::string& counterName,
                                  TimeRange range,
                                  double pct);
    static void readAllValue(folly::dynamic& vals);


    void prometheusSerialize(std::ostream& out) /*const*/ override;

    static StatsManager& get();

private:
    static constexpr bool isStatIndex(int32_t i);
    static constexpr bool isHistoIndex(int32_t i);
    static constexpr std::size_t physicalStatIndex(int32_t i);
    static constexpr std::size_t physicalHistoIndex(int32_t i);
    static StatusOr<StatsManager::ParsedName>
    parseMetricName(folly::StringPiece metricName);

    StatsManager() = default;
    StatsManager(const StatsManager&) = delete;
    StatsManager(StatsManager&&) = delete;

    template<class StatsHolder>
    static VT readValue(StatsHolder& stats, TimeRange range, StatsMethod method);


private:
    std::string domain_;
    HostAddr collectorAddr_{0, 0};
    int32_t interval_{0};

    // <counter_name> => index
    // when index > 0, (index - 1) is the index of stats_ list
    // when index < 0, [- (index + 1)] is the index of histograms_ list
    std::unordered_map<std::string, int32_t> nameMap_;

    // All time series stats
    std::vector<
        std::pair<std::unique_ptr<std::mutex>,
                  std::unique_ptr<StatsType>
        >
    > stats_;

    // All histogram stats
    std::vector<
        std::pair<std::unique_ptr<std::mutex>,
                  std::unique_ptr<HistogramType>
        >
    > histograms_;
};

}  // namespace stats
}  // namespace nebula

#include "stats/StatsManager.inl"

#endif  // COMMON_STATS_STATSMANAGER_H_
