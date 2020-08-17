/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DataCollectExecutor.h"

#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataCollectExecutor::execute() {
    return doCollect().ensure([this] () {
        result_ = Value::kEmpty;
        colNames_.clear();
    });
}

folly::Future<Status> DataCollectExecutor::doCollect() {
    SCOPED_TIMER(&execTime_);

    auto* dc = asNode<DataCollect>(node());
    colNames_ = dc->colNames();
    auto vars = dc->vars();
    switch (dc->collectKind()) {
        case DataCollect::CollectKind::kSubgraph: {
            NG_RETURN_IF_ERROR(collectSubgraph(vars));
            break;
        }
        case DataCollect::CollectKind::kRowBasedMove: {
            NG_RETURN_IF_ERROR(rowBasedMove(vars));
            break;
        }
        case DataCollect::CollectKind::kMToN: {
            NG_RETURN_IF_ERROR(collectMToN(vars, dc->mToN(), dc->distinct()));
            break;
        }
        default:
            LOG(FATAL) << "Unknown data collect type: " << static_cast<int64_t>(dc->collectKind());
    }
    ResultBuilder builder;
    builder.value(Value(std::move(result_))).iter(Iterator::Kind::kSequential);
    return finish(builder.finish());
}

Status DataCollectExecutor::collectSubgraph(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    // the subgraph not need duplicate vertices or edges, so dedup here directly
    std::unordered_set<std::string> vids;
    std::unordered_set<std::tuple<std::string, int64_t, int64_t, std::string>> edgeKeys;
    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        for (auto& result : hist) {
            auto iter = result.iter();
            if (iter->isGetNeighborsIter()) {
                List vertices;
                List edges;
                auto* gnIter = static_cast<GetNeighborsIter*>(iter.get());
                auto originVertices = gnIter->getVertices();
                for (auto& v : originVertices.values) {
                    if (!v.isVertex()) {
                        continue;
                    }
                    if (vids.emplace(v.getVertex().vid).second) {
                        vertices.emplace_back(std::move(v));
                    }
                }
                auto originEdges = gnIter->getEdges();
                for (auto& e : originEdges.values) {
                    if (!e.isEdge()) {
                        continue;
                    }
                    auto edgeKey = std::make_tuple(e.getEdge().src,
                                                   e.getEdge().type,
                                                   e.getEdge().ranking,
                                                   e.getEdge().dst);
                    if (edgeKeys.emplace(std::move(edgeKey)).second) {
                        edges.emplace_back(std::move(e));
                    }
                }
                ds.rows.emplace_back(Row({std::move(vertices), std::move(edges)}));
            } else {
                return Status::Error("Iterator should be kind of GetNeighborIter.");
            }
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::rowBasedMove(const std::vector<std::string>& vars) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());
    for (auto& var : vars) {
        auto& result = ectx_->getResult(var);
        auto iter = result.iter();
        if (iter->isSequentialIter()) {
            auto* seqIter = static_cast<SequentialIter*>(iter.get());
            for (; seqIter->valid(); seqIter->next()) {
                ds.rows.emplace_back(seqIter->moveRow());
            }
        } else {
            return Status::Error("Iterator should be kind of SequentialIter.");
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}

Status DataCollectExecutor::collectMToN(const std::vector<std::string>& vars,
                                        StepClause::MToN* mToN,
                                        bool distinct) {
    DataSet ds;
    ds.colNames = std::move(colNames_);
    DCHECK(!ds.colNames.empty());
    std::unordered_set<const LogicalRow*> unique;
    // itersHolder keep life cycle of iters util this method return.
    std::vector<std::unique_ptr<Iterator>> itersHolder;
    for (auto& var : vars) {
        auto& hist = ectx_->getHistory(var);
        DCHECK_GE(mToN->mSteps, 1);
        for (auto i = mToN->mSteps - 1; i < mToN->nSteps; ++i) {
            auto iter = hist[i].iter();
            if (iter->isSequentialIter()) {
                auto* seqIter = static_cast<SequentialIter*>(iter.get());
                for (; seqIter->valid(); seqIter->next()) {
                    if (distinct && !unique.emplace(seqIter->row()).second) {
                            continue;
                    }
                    ds.rows.emplace_back(seqIter->moveRow());
                }
            } else {
                return Status::Error("Iterator should be kind of SequentialIter.");
            }
            itersHolder.emplace_back(std::move(iter));
        }
    }
    result_.setDataSet(std::move(ds));
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
