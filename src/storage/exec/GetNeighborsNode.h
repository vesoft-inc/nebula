/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETNEIGHBORSNODE_H_
#define STORAGE_EXEC_GETNEIGHBORSNODE_H_

#include "common/base/Base.h"
#include "common/algorithm/ReservoirSampling.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/StorageFlags.h"


namespace nebula {
namespace storage {

// GetNeighborsNode will generate a row in response of GetNeighbors, so it need to get the tag
// result from HashJoinNode, and the stat info and edge iterator from AggregateNode. Then collect
// some edge props, and put them into the target cell of a row.
class GetNeighborsNode : public QueryNode<VertexID> {
public:
    using RelNode::execute;

    GetNeighborsNode(RunTimeContext* context,
                     IterateNode<VertexID>* hashJoinNode,
                     IterateNode<VertexID>* upstream,
                     EdgeContext* edgeContext,
                     nebula::DataSet* resultDataSet,
                     int64_t limit = 0)
        : context_(context)
        , hashJoinNode_(hashJoinNode)
        , upstream_(upstream)
        , edgeContext_(edgeContext)
        , resultDataSet_(resultDataSet)
        , limit_(limit) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
        }

        std::vector<Value> row;
        // vertexId is the first column
        if (context_->isIntId()) {
            row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
            row.emplace_back(vId);
        }
        // second column is reserved for stat
        row.emplace_back(Value());

        auto tagResult = hashJoinNode_->result().getList();
        for (auto& value : tagResult.values) {
            row.emplace_back(std::move(value));
        }

        // add default null for each edge node and the last column of yield expression
        row.resize(row.size() + edgeContext_->propContexts_.size() + 1, Value());

        ret = iterateEdges(row);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (edgeContext_->statCount_ > 0) {
            auto agg = dynamic_cast<AggregateNode<VertexID>*>(upstream_);
            CHECK_NOTNULL(agg);
            agg->calculateStat();
            // set stat list to second columns
            row[1].setList(agg->mutableResult().moveList());
        }

        resultDataSet_->rows.emplace_back(std::move(row));
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

protected:
    GetNeighborsNode() = default;

    virtual nebula::cpp2::ErrorCode iterateEdges(std::vector<Value>& row) {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; upstream_->valid(); upstream_->next(), ++edgeRowCount) {
            if (edgeRowCount >= limit_) {
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            }
            auto key = upstream_->key();
            auto reader = upstream_->reader();
            auto props = context_->props_;
            auto columnIdx = context_->columnIdx_;

            list.reserve(props->size());
            // collect props need to return
            if (!QueryUtils::collectEdgeProps(key, context_->vIdLen(), context_->isIntId(),
                                              reader, props, list).ok()) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }

            // add edge prop value to the target column
            if (row[columnIdx].empty()) {
                row[columnIdx].setList(nebula::List());
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    RunTimeContext* context_;
    IterateNode<VertexID>* hashJoinNode_;
    IterateNode<VertexID>* upstream_;
    EdgeContext* edgeContext_;
    nebula::DataSet* resultDataSet_;
    int64_t limit_;
};

class GetNeighborsSampleNode : public GetNeighborsNode {
public:
    GetNeighborsSampleNode(RunTimeContext* context,
                           IterateNode<VertexID>* hashJoinNode,
                           IterateNode<VertexID>* upstream,
                           EdgeContext* edgeContext,
                           nebula::DataSet* resultDataSet,
                           int64_t limit)
        : GetNeighborsNode(context, hashJoinNode, upstream, edgeContext, resultDataSet, limit) {
            sampler_ = std::make_unique<nebula::algorithm::ReservoirSampling<Sample>>(limit);
        }

private:
    using Sample = std::tuple<EdgeType,
                              std::string,
                              std::string,
                              const std::vector<PropContext>*,
                              size_t>;

    nebula::cpp2::ErrorCode iterateEdges(std::vector<Value>& row) override {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; upstream_->valid(); upstream_->next(), ++edgeRowCount) {
            auto val = upstream_->val();
            auto key = upstream_->key();
            auto edgeType = context_->edgeType_;
            auto props = context_->props_;
            auto columnIdx = context_->columnIdx_;
            sampler_->sampling(std::make_tuple(edgeType, val.str(), key.str(), props, columnIdx));
        }

        RowReaderWrapper reader;
        auto samples = std::move(*sampler_).samples();
        for (auto& sample : samples) {
            auto columnIdx = std::get<4>(sample);
            // add edge prop value to the target column
            if (row[columnIdx].empty()) {
                row[columnIdx].setList(nebula::List());
            }

            auto edgeType = std::get<0>(sample);
            const auto& val = std::get<1>(sample);
            reader = RowReaderWrapper::getEdgePropReader(context_->env()->schemaMan_,
                                                         context_->spaceId(),
                                                         std::abs(edgeType),
                                                         val);
            if (!reader) {
                continue;
            }

            const auto& key = std::get<2>(sample);
            const auto& props = std::get<3>(sample);
            if (!QueryUtils::collectEdgeProps(key, context_->vIdLen(), context_->isIntId(),
                                              reader.get(), props, list).ok()) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }

        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    std::unique_ptr<nebula::algorithm::ReservoirSampling<Sample>> sampler_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
