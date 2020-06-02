/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETNEIGHBORSNODE_H_
#define STORAGE_EXEC_GETNEIGHBORSNODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/StatCollector.h"

namespace nebula {
namespace storage {

class GetNeighborsNode : public QueryNode<VertexID> {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    GetNeighborsNode(FilterNode* filterNode,
                     EdgeContext* edgeContext)
        : filterNode_(filterNode)
        , edgeContext_(edgeContext) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        result_.setList(nebula::List());
        auto& result = result_.mutableList();
        // vertexId is the first column
        result.values.emplace_back(vId);
        // reserve second column for stat
        result.values.emplace_back(NullType::__NULL__);
        if (edgeContext_->statCount_ > 0) {
            stats_ = statProcessor_.initStatValue(edgeContext_);
        }

        auto tagResult = filterNode_->result().getList();
        for (auto& value : tagResult.values) {
            result.values.emplace_back(std::move(value));
        }

        // add default null for each edge node
        result.values.resize(result.values.size() + edgeContext_->propContexts_.size(),
                               NullType::__NULL__);
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; filterNode_->valid(); filterNode_->next(), ++edgeRowCount) {
            auto srcId = filterNode_->srcId();
            auto edgeType = filterNode_->edgeType();
            auto edgeRank = filterNode_->edgeRank();
            auto dstId = filterNode_->dstId();
            auto reader = filterNode_->reader();
            auto props = filterNode_->props();
            auto columnIdx = filterNode_->idx();

            // collect props need to return
            ret = collectEdgeProps(srcId, edgeType, edgeRank, dstId, reader, props, list);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            // add edge prop value to the target column
            if (result.values[columnIdx].type() == Value::Type::NULLVALUE) {
                result.values[columnIdx].setList(nebula::List());
            }
            auto& cell = result.values[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));

            // collect edge stat if has stat to return
            if (edgeContext_->statCount_ > 0) {
                statProcessor_.collectEdgeStats(srcId, edgeType, edgeRank, dstId,
                                                reader, props, stats_);
            }
        }

        if (edgeContext_->statCount_ > 0) {
            auto stats = statProcessor_.calculateStat(stats_);
            result.values[1].setList(std::move(stats));
        }

        DVLOG(1) << vId << " process " << edgeRowCount << " edges in total.";
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    GetNeighborsNode() = default;

    FilterNode* filterNode_;
    EdgeContext* edgeContext_;
    StatCollector statProcessor_;
    std::vector<PropStat> stats_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
