/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_STATS_STATSMANAGER_H_
#define COMMON_STATS_STATSMANAGER_H_

#include "common/base/Base.h"
#include <folly/RWSpinLock.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/stats/TimeseriesHistogram.h>
#include "common/datatypes/HostAddr.h"
#include "common/time/WallClock.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace stats {

// A wrapper class of counter index. Each instance can only be writtern once.
class CounterId final {
public:
    CounterId() : index_{0} {}
    CounterId(int32_t index) : index_{index} {}  // NOLINT
    CounterId(const CounterId &) = default;

    CounterId& operator=(const CounterId& right) {
        if (!right.valid()) {
            LOG(FATAL) << "Invalid counter id";
        }
        if (valid()) {
            // Already assigned
            LOG(FATAL) << "CounterId cannot be assigned twice";
        }
        index_ = right.index_;
        return *this;
    }

    CounterId& operator=(int32_t right) {
        if (right == 0) {
            LOG(FATAL) << "Invalid counter id";
        }
        if (valid()) {
            // Already assigned
            LOG(FATAL) << "CounterId cannot be assigned twice";
        }
        index_ = right;
        return *this;
    }

    bool valid() const {
        return index_ != 0;
    }

    int32_t index() const {
        return index_;
    }

private:
    int32_t index_;
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
 *   query.rate.5       -- query per second in the five seconds
 *   query.rate.60      -- query per second in the last minute
 *   latency.p99.600    -- The latency that slower than 99% of all queries
 *                           in the last ten minutes
 *   latency.p9999.60   -- The latency that slower than 99.99% of all queries
 *                           in the last one minute
 *   error.count.600    -- Total number of errors in the last ten minutes
 */
class StatsManager final {
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
        FIVE_SECONDS = 0,
        ONE_MINUTE = 1,
        TEN_MINUTES = 2,
        ONE_HOUR = 3
    };

    static void setDomain(folly::StringPiece domain);
    // addr     -- The ip/port of the stats collector. StatsManager will periodically
    //             report the stats to the collector
    // interval -- The number of seconds between each report
    static void setReportInfo(HostAddr addr, int32_t interval);

    // Both register methods return the index to the internal data structure.
    // This index will be used by addValue() methods.
    //
    // Both register methods is not thread safe, and user need to register stats one
    // by one before calling addValue, readStats, readHisto and so on.
    //
    // The parameter **stats** is a list of statistic method abbreviations (or
    // percentiles when registering histogram), separated by commas, such as
    // "avg, rate, sum", or "avg, rate, p95, p99". This parameter only affects
    // the readAllValue() method. The readAllValue() only returns the matrix for
    // those specified in the parameter **stats**. If **stats** is empty, nothing
    // will return from readAllValue()
    static CounterId registerStats(folly::StringPiece counterName, std::string stats);
    static CounterId registerHisto(folly::StringPiece counterName,
                                   VT bucketSize,
                                   VT min,
                                   VT max,
                                   std::string stats);

    static void addValue(const CounterId& id, VT value = 1);

    // The parameter counter here must be a qualified counter name, which includes
    // all three parts (counter name, method/percentile, and time range). Here are
    // some examples:
    //
    //    query_qps.rate.60
    //    query_latency.p95.600
    //    query_latency.avg.60
    static StatusOr<VT> readValue(folly::StringPiece counter);

    static StatusOr<VT> readStats(const CounterId& id,
                                  TimeRange range,
                                  StatsMethod method);
    static StatusOr<VT> readStats(const std::string& counterName,
                                  TimeRange range,
                                  StatsMethod method);
    static StatusOr<VT> readHisto(const CounterId& id,
                                  TimeRange range,
                                  double pct);
    static StatusOr<VT> readHisto(const std::string& counterName,
                                  TimeRange range,
                                  double pct);
    static void readAllValue(folly::dynamic& vals);


private:
    static StatsManager& get();

    StatsManager() = default;
    StatsManager(const StatsManager&) = delete;
    StatsManager(StatsManager&&) = delete;

    static bool strToPct(folly::StringPiece part, double& pct);
    static void parseStats(const folly::StringPiece stats,
                           std::vector<StatsMethod>& methods,
                           std::vector<std::pair<std::string, double>>& percentiles);

    template<class StatsHolder>
    static VT readValue(StatsHolder& stats, TimeRange range, StatsMethod method);


private:
    struct CounterInfo {
        CounterId                                   id_;
        std::vector<StatsMethod>                    methods_;
        std::vector<std::pair<std::string, double>> percentiles_;

        CounterInfo(int32_t index,
                    std::vector<StatsMethod>&& methods,
                    std::vector<std::pair<std::string, double>>&& percentiles)
            : id_(index)
            , methods_(std::move(methods))
            , percentiles_(std::move(percentiles)) {}
    };

    std::string domain_;
    HostAddr collectorAddr_{"", 0};
    int32_t interval_{0};

    // <counter_name> => index
    // when index > 0, (index - 1) is the index of stats_ list
    // when index < 0, [- (index + 1)] is the index of histograms_ list
    folly::RWSpinLock nameMapLock_;
    std::unordered_map<std::string, CounterInfo> nameMap_;

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

#include "common/stats/StatsManager.inl"

#endif  // COMMON_STATS_STATSMANAGER_H_
