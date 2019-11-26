/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METASTATS_H_
#define META_METASTATS_H_

#include "stats/StatsManager.h"

namespace nebula {
namespace meta {

struct MetaStats {
    MetaStats() = default;

    explicit MetaStats(const std::string& name) {
        qpsStatId_ = stats::StatsManager::registerStats(name + "_qps");
        errorQpsStatId_ = stats::StatsManager::registerStats(name + "_error_qps");
        latencyStatId_ = stats::StatsManager::registerHisto(name + "_latency", 100, 1, 1000 * 1000);
    }

    int32_t qpsStatId_;
    int32_t errorQpsStatId_;
    int32_t latencyStatId_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METASTATS_H_
