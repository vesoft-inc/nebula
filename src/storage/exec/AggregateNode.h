/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_AGGREGATENODE_H_
#define STORAGE_EXEC_AGGREGATENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/GetNeighborsNode.h"

namespace nebula {
namespace storage {

// AggregateNode is the node which would generate a DataSet of Response, most likely it would use
// the result of QueryNode, do some works, and then put it into the output DataSet.
// Some other AggregateNode will receive the whole DataSet, do some works, and output again, such
// as SortNode and GroupByNode
template<typename T>
class AggregateNode : public RelNode<T> {
public:
    AggregateNode(QueryNode<T>* node,
                  nebula::DataSet* result)
        : node_(node)
        , result_(result) {}

    explicit AggregateNode(nebula::DataSet* result)
        : result_(result) {}

    kvstore::ResultCode execute(PartitionID partId, const T& input) override {
        auto ret = RelNode<T>::execute(partId, input);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // The AggregateNode just get the result of QueryNode, add it to DataSet
        const auto& values = node_->result().getList().values;
        result_->rows.emplace_back(std::move(values));
        return kvstore::ResultCode::SUCCEEDED;
    }

protected:
    QueryNode<T>* node_;
    nebula::DataSet* result_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_AGGREGATENODE_H_
