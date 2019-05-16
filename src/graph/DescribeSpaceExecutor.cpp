/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DescribeSpaceExecutor.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace graph {

DescribeSpaceExecutor::DescribeSpaceExecutor(Sentence *sentence,
                                             ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
}

Status DescribeSpaceExecutor::prepare() {
    return Status::OK();;
}

void DescribeSpaceExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->spaceName();
    auto spaceId = ectx()->schemaManager()->toGraphSpaceID(*name);
    auto spaceItem = mc->getSpace(spaceId).get();
    if (!spaceItem.ok()) {
        onError_(Status::Error("Space not found `%d`", spaceId));
        return;
    }

    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    std::vector<std::string> header{"ID", "Name", "Partition number", "Replica Factor"};
    resp_->set_column_names(std::move(header));

    std::vector<cpp2::RowValue> rows;
    std::vector<cpp2::ColumnValue> row;
    row.resize(4);
    row[0].set_integer(spaceItem.value().get_space_id());

    auto properties = spaceItem.value().get_properties();
    row[1].set_str(properties.get_space_name());
    row[2].set_integer(properties.get_partition_num());
    row[3].set_integer(properties.get_replica_factor());

    rows.emplace_back();
    rows.back().set_columns(std::move(row));
    resp_->set_rows(std::move(rows));
    DCHECK(onFinish_);
    onFinish_();
}

void DescribeSpaceExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
