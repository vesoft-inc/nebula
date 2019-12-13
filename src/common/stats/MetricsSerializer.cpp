/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "MetricsSerializer.h"

namespace nebula {
namespace stats {

// The raw metric format based on JSON
// 1. Gauge, time serial value
// 2. Histogram, time serial value distribution
// The raw data can be transformed to multiple specified foramt (various user-defined metric format)
// E.G.
// {
//     "Gauges": [...],
//     "Histograms:" [...],
// }
// Gauge
// {
//     "name": "xxxxx",
//     "value": 33,
//     "labels": ["name:nebula", "type:qps"]  // key:value
// }
// Histogram
// {
//     "name": "xxxxx",
//     "value_range": [0, 100],
//     "buckets": [2, 3, 0, 11, ...],
//     "labels": ["name:nebula", "type:latency"]
// }
//
void MetricsSerializer::serialize(std::ostream& out, StatsManager& sm) const {
    constexpr char kGauges[] = "Gauges";
    constexpr char kHistograms[] = "Histograms";
    folly::dynamic obj = folly::dynamic::object(kGauges, folly::dynamic::array())
        (kHistograms, folly::dynamic::array());

    // insert
    for (auto& index : sm.nameMap_) {
        auto parsedName = sm.idParsedName_[index.second].get();
        std::string name = parsedName != nullptr ? parsedName->name : index.first;
        if (StatsManager::isStatIndex(index.second)) {
            folly::dynamic gauge = folly::dynamic::object("name", name)
                    ("value", StatsManager::readStats(index.second,
                        StatsManager::TimeRange::ONE_MINUTE,
                        StatsManager::StatsMethod::AVG).value())
                    ("labels", folly::dynamic::array("name:nebula"));
            obj[kGauges].push_back(std::move(gauge));
        } else if (StatsManager::isHistoIndex(index.second)) {
            auto& p = sm.histograms_[StatsManager::physicalHistoIndex(index.second)];
            std::lock_guard<std::mutex> lk(*p.first);
            auto& hist = p.second;
            folly::dynamic buckets = folly::dynamic::array();
            for (std::size_t i = 0; i < hist->getNumBuckets(); ++i) {
                if (parsedName != nullptr) {
                    buckets.push_back(
                        hist->getBucket(i).count(static_cast<std::size_t>(parsedName->range)));
                } else {
                    buckets.push_back(
                        hist->getBucket(i).count(static_cast<std::size_t>(
                            StatsManager::TimeRange::ONE_HOUR)));
                }
            }
            folly::dynamic histogram = folly::dynamic::object("name", name)
                ("value_range", folly::dynamic::array(hist->getMin(), hist->getMax()))
                ("buckets", std::move(buckets))
                ("lables", folly::dynamic::array("name:nebula"));
            obj[kHistograms].push_back(std::move(histogram));
        }
    }
    out << folly::toJson(obj);
}

}  // namespace stats
}  // namespace nebula
