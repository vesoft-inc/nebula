/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/DescribeSpaceExecutor.h"

namespace nebula {
namespace graph {

DescribeSpaceExecutor::DescribeSpaceExecutor(Sentence *sentence,
                                             ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
}

Status DescribeSpaceExecutor::prepare() {
    return Status::OK();
}

void DescribeSpaceExecutor::execute() {
    auto *name = sentence_->name();
    auto future = ectx()->getMetaClient()->getSpace(*name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(Status::Error("Space not found"));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"ID", "Name", "Partition number", "Replica Factor"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(4);
        row[0].set_integer(resp.value().get_space_id());

        auto properties = resp.value().get_properties();
        row[1].set_str(properties.get_space_name());
        row[2].set_integer(properties.get_partition_num());
        row[3].set_integer(properties.get_replica_factor());

        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DescribeSpaceExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
