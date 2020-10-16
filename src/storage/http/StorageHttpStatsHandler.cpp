/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "kvstore/RocksEngineConfig.h"
#include "storage/http/StorageHttpStatsHandler.h"
#include <proxygen/lib/http/ProxygenErrorEnum.h>

namespace nebula {
namespace storage {

using proxygen::ProxygenError;

void StorageHttpStatsHandler::onError(ProxygenError err) noexcept {
    LOG(ERROR) << "Web service StorageHttpStatsHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}

folly::dynamic StorageHttpStatsHandler::getStats() const {
    auto stats = folly::dynamic::array();
    std::shared_ptr<rocksdb::Statistics> statistics = kvstore::getDBStatistics();
    if (statistics) {
        std::map<std::string, uint64_t> stats_map;
        if (statistics->getTickerMap(&stats_map)) {
            for (const auto& stat : stats_map) {
                if (!statFiltered(stat.first)) {
                    addOneStat(stats, stat.first, stat.second);
                }
            }
        }
        if (statistics->get_stats_level() > rocksdb::StatsLevel::kExceptHistogramOrTimers) {
            for (const auto& h : rocksdb::HistogramsNameMap) {
                if (!statFiltered(h.second)) {
                    rocksdb::HistogramData hData;
                    statistics->histogramData(h.first, &hData);
                    addOneStat(stats, h.second + ".p50", hData.average);
                    addOneStat(stats, h.second + ".p95", hData.percentile95);
                    addOneStat(stats, h.second + ".p99", hData.percentile99);
                }
            }
        }
    }
    return stats;
}

bool StorageHttpStatsHandler::statFiltered(const std::string& stat) const {
    if (statNames_.empty()) {
        return false;
    }
    return std::find(statNames_.begin(), statNames_.end(), stat) == statNames_.end();
}

}  // namespace storage
}  // namespace nebula
