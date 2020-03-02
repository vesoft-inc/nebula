/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DescribeTagIndexExecutor.h"

namespace nebula {
namespace graph {

DescribeTagIndexExecutor::DescribeTagIndexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeTagIndexSentence*>(sentence);
}

Status DescribeTagIndexExecutor::prepare() {
    return Status::OK();
}


void DescribeTagIndexExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto *name = sentence_->indexName();
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->getTagIndex(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Index \"%s\" not found",
                                   sentence_->indexName()->c_str()));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"Field", "Type"};
        resp_->set_column_names(std::move(header));
        std::vector<cpp2::RowValue> rows;
        auto& fields = resp.value().get_fields();
        for (auto field : fields) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(field.name);
            row[1].set_str(valueTypeToString(field.type));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }

        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void DescribeTagIndexExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}


}   // namespace graph
}   // namespace nebula

