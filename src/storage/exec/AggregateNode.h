
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_AGGREGATENODE_H_
#define STORAGE_EXEC_AGGREGATENODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"

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

// AggregateNode will only be used in GetNeighbors for now, it need to calculate some stat of all
// valid edges of a vertex. It could be used in ScanVertex or ScanEdge later.
// The stat is collected during we iterate over edges via `next`, so if you want to get the
// final result, be sure to call `calculateStat` and then retrieve the reuslt
template<typename T>
class AggregateNode : public IterateNode<T> {
public:
    AggregateNode(PlanContext* planCtx,
                  IterateNode<T>* upstream,
                  EdgeContext* edgeContext)
        : IterateNode<T>(upstream)
        , planContext_(planCtx)
        , edgeContext_(edgeContext) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        if (edgeContext_->statCount_ > 0) {
            initStatValue(edgeContext_);
        }
        this->result_ = Value();
        return kvstore::ResultCode::SUCCEEDED;
    }

    void next() override {
        if (!stats_.empty()) {
            // we need to collect the stat during `next`
            collectEdgeStats(srcId(), edgeType(), edgeRank(), dstId(),
                             this->reader(), planContext_->props_);
        }
        IterateNode<T>::next();
    }

    void calculateStat() {
        if (stats_.empty()) {
            return;
        }
        nebula::List result;
        result.values.reserve(stats_.size());
        for (const auto& stat : stats_) {
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
        this->result_.setList(std::move(result));
    }

private:
    VertexIDSlice srcId() const {
        return NebulaKeyUtils::getSrcId(planContext_->vIdLen_, this->key());
    }

    EdgeType edgeType() const {
        return NebulaKeyUtils::getEdgeType(planContext_->vIdLen_, this->key());
    }

    EdgeRanking edgeRank() const {
        return NebulaKeyUtils::getRank(planContext_->vIdLen_, this->key());
    }

    VertexIDSlice dstId() const {
        return NebulaKeyUtils::getDstId(planContext_->vIdLen_, this->key());
    }

    void initStatValue(EdgeContext* edgeContext) {
        stats_.clear();
        // initialize all stat value of all edgeTypes
        if (edgeContext->statCount_ > 0) {
            stats_.resize(edgeContext->statCount_);
            for (const auto& ec : edgeContext->propContexts_) {
                for (const auto& ctx : ec.second) {
                    if (ctx.hasStat_) {
                        for (size_t i = 0; i < ctx.statType_.size(); i++) {
                            PropStat stat(ctx.statType_[i]);
                            stats_[ctx.statIndex_[i]] = std::move(stat);
                        }
                    }
                }
            }
        }
    }

    kvstore::ResultCode collectEdgeStats(VertexIDSlice srcId,
                                         EdgeType edgeType,
                                         EdgeRanking edgeRank,
                                         VertexIDSlice dstId,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props) {
        for (const auto& prop : *props) {
            if (prop.hasStat_) {
                for (const auto statIndex : prop.statIndex_) {
                    VLOG(2) << "Collect stat prop " << prop.name_ << ", type " << edgeType;
                    auto value = QueryUtils::readEdgeProp(srcId, edgeType, edgeRank, dstId,
                                                          reader, prop);
                    if (!value.ok()) {
                        return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                    }
                    addStatValue(std::move(value).value(), stats_[statIndex]);
                }
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    void addStatValue(const Value& value, PropStat& stat) {
        if (stat.statType_ == cpp2::StatType::SUM || stat.statType_ == cpp2::StatType::AVG) {
            stat.sum_ = stat.sum_ + value;
            stat.count_ = stat.count_ + 1;
        } else if (stat.statType_ == cpp2::StatType::COUNT) {
            stat.count_ = stat.count_ + 1;
        } else if (stat.statType_ == cpp2::StatType::MAX) {
            stat.max_ = value > stat.max_ ? value : stat.max_;
        } else if (stat.statType_ == cpp2::StatType::MIN) {
            stat.min_ = value < stat.min_ ? value : stat.min_;
        }
    }

private:
    PlanContext* planContext_;
    EdgeContext* edgeContext_;
    std::vector<PropStat> stats_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_AGGREGATENODE_H_
