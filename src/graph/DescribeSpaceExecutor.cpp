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
                                             ExecutionContext *ectx)
    : Executor(ectx, "describe_space") {
    sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
}

Status DescribeSpaceExecutor::prepare() {
    return Status::OK();
}

void DescribeSpaceExecutor::execute() {
    auto *name = sentence_->spaceName();
    auto future = ectx()->getMetaClient()->getSpace(*name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Space not found"));
            return;
        }
        /**
        * Permission check.
        */
        auto spaceId = resp.value().get_space_id();

        auto *session = ectx()->rctx()->session();
        auto rst = permission::PermissionManager::canReadSpace(session, spaceId);
        if (!rst) {
            doError(Status::PermissionError("Permission denied"));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"ID",
                                        "Name",
                                        "Partition number",
                                        "Replica Factor",
                                        "Charset",
                                        "Collate"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(6);
        row[0].set_integer(spaceId);

        auto properties = resp.value().get_properties();
        row[1].set_str(properties.get_space_name());
        row[2].set_integer(properties.get_partition_num());
        row[3].set_integer(properties.get_replica_factor());
        row[4].set_str(properties.get_charset_name());
        row[5].set_str(properties.get_collate_name());

        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Describe space exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DescribeSpaceExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
