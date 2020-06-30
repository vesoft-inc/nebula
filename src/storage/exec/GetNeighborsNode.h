/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETNEIGHBORSNODE_H_
#define STORAGE_EXEC_GETNEIGHBORSNODE_H_

#include "common/base/Base.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/HashJoinNode.h"

namespace nebula {
namespace storage {

// GetNeighborsNode will generate a row in response of GetNeighbors, so it need to get the tag
// result from HashJoinNode, and the stat info and edge iterator from AggregateNode. Then collect
// some edge props, and put them into the target cell of a row.
class GetNeighborsNode : public QueryNode<VertexID> {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    GetNeighborsNode(PlanContext* planCtx,
                     HashJoinNode* hashJoinNode,
                     AggregateNode* AggregateNode,
                     EdgeContext* edgeContext,
                     nebula::DataSet* resultDataSet)
        : planContext_(planCtx)
        , hashJoinNode_(hashJoinNode)
        , aggregateNode_(AggregateNode)
        , edgeContext_(edgeContext)
        , resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        std::vector<Value> row;
        // vertexId is the first column
        row.emplace_back(vId);
        // second column is reserved for stat
        row.emplace_back(NullType::__NULL__);

        auto tagResult = hashJoinNode_->result().getList();
        for (auto& value : tagResult.values) {
            row.emplace_back(std::move(value));
        }

        // add default null for each edge node and the last column of yield expression
        row.resize(row.size() + edgeContext_->propContexts_.size() + 1,
                   NullType::__NULL__);
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; aggregateNode_->valid(); aggregateNode_->next(), ++edgeRowCount) {
            auto edgeType = aggregateNode_->edgeType();
            auto key = aggregateNode_->key();
            auto reader = aggregateNode_->reader();
            auto props = aggregateNode_->props();
            auto columnIdx = aggregateNode_->idx();
            const auto& edgeName = aggregateNode_->edgeName();

            // collect props need to return
            ret = collectEdgeProps(edgeType,
                                   edgeName,
                                   reader,
                                   key,
                                   planContext_->vIdLen_,
                                   props,
                                   list);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            // add edge prop value to the target column
            if (row[columnIdx].type() == Value::Type::NULLVALUE) {
                row[columnIdx].setList(nebula::List());
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }

        aggregateNode_->calculateStat();
        if (aggregateNode_->result().type() == Value::Type::LIST) {
            // set stat list to second columns
            row[1].setList(aggregateNode_->mutableResult().moveList());
        }

        DVLOG(1) << vId << " process " << edgeRowCount << " edges in total.";
        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    GetNeighborsNode() = default;

    PlanContext* planContext_;
    HashJoinNode* hashJoinNode_;
    AggregateNode* aggregateNode_;
    EdgeContext* edgeContext_;
    nebula::DataSet* resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
