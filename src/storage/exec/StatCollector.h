
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_STATCOLLECTOR_H_
#define STORAGE_EXEC_STATCOLLECTOR_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

// used to save stat value of each vertex
struct PropStat {
    PropStat() = default;

    explicit PropStat(const cpp2::StatType& statType) : statType_(statType) {}

    cpp2::StatType statType_;
    mutable Value sum_ = 0L;
    mutable Value count_ = 0L;
    mutable Value min_ = std::numeric_limits<int64_t>::max();
    mutable Value max_ = std::numeric_limits<int64_t>::min();
};

// StatCollector will only be used in GetNeighbors for now, it need to calculate some stat of all
// valid edges of a vertex. It could be used in ScanVertex or ScanEdge later.
class StatCollector {
public:
    std::vector<PropStat> initStatValue(EdgeContext* edgeContext) {
        // initialize all stat value of all edgeTypes
        std::vector<PropStat> stats;
        if (edgeContext->statCount_ > 0) {
            stats.resize(edgeContext->statCount_);
            for (const auto& ec : edgeContext->propContexts_) {
                for (const auto& ctx : ec.second) {
                    if (ctx.hasStat_) {
                        PropStat stat(ctx.statType_);
                        stats[ctx.statIndex_] = std::move(stat);
                    }
                }
            }
        }
        return stats;
    }

    kvstore::ResultCode collectEdgeStats(VertexIDSlice srcId,
                                         EdgeType edgeType,
                                         EdgeRanking edgeRank,
                                         VertexIDSlice dstId,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props,
                                         std::vector<PropStat>& stats) {
        for (size_t i = 0; i < props->size(); i++) {
            const auto& prop = (*props)[i];
            if (prop.hasStat_) {
                VLOG(2) << "Collect stat prop " << prop.name_ << ", type " << edgeType;
                auto value = QueryUtils::readEdgeProp(srcId, edgeType, edgeRank, dstId,
                                                      reader, prop);
                addStatValue(value, stats[prop.statIndex_]);
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    nebula::List calculateStat(const std::vector<PropStat>& stats) {
        nebula::List result;
        result.values.reserve(stats.size());
        for (const auto& stat : stats) {
            if (stat.statType_ == cpp2::StatType::SUM) {
                result.values.emplace_back(stat.sum_);
            } else if (stat.statType_ == cpp2::StatType::COUNT) {
                result.values.emplace_back(stat.count_);
            } else if (stat.statType_ == cpp2::StatType::AVG) {
                result.values.emplace_back(stat.sum_ / stat.count_);
            } else if (stat.statType_ == cpp2::StatType::MAX) {
                result.values.emplace_back(stat.max_);
            } else if (stat.statType_ == cpp2::StatType::MIN) {
                result.values.emplace_back(stat.min_);
            }
        }
        return result;
    }

private:
    void addStatValue(const Value& value, PropStat& stat) {
        stat.sum_ = stat.sum_ + value;
        stat.count_ = stat.count_ + 1;
        stat.max_ = value > stat.max_ ? value : stat.max_;
        stat.min_ = value < stat.min_ ? value : stat.min_;
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STATCOLLECTOR_H_
