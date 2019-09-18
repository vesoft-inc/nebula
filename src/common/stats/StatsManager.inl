/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {
namespace stats {

// static
template<class StatsHolder>
StatsManager::VT StatsManager::readValue(StatsHolder& stats,
                                         StatsManager::TimeRange range,
                                         StatsManager::StatsMethod method) {
    size_t level = static_cast<size_t>(range);
    switch (method) {
        case StatsMethod::SUM:
            return stats.sum(level);
        case StatsMethod::COUNT:
            return stats.count(level);
        case StatsMethod::AVG:
            return stats.template avg<VT>(level);
        case StatsMethod::RATE:
            return stats.template rate<VT>(level);
    }

    LOG(FATAL) << "Unknown statistic method";
}

}  // namespace stats
}  // namespace nebula

