/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/DescribeTagExecutor.h"

namespace vesoft {
namespace vgraph {

DescribeTagExecutor::DescribeTagExecutor(Sentence *sentence,
                                         ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeTagSentence*>(sentence);
}


Status DescribeTagExecutor::prepare() {
    return Status::OK();
}


void DescribeTagExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    auto *schema = sm->getTagSchema(*name);
    do {
        if (schema == nullptr) {
            onError_(Status::Error("Schema not found for tag `%s'", name->c_str()));
            return;
        }
        std::vector<std::string> header{"Column", "Type"};
        resp_->set_column_names(std::move(header));
        auto &items = schema->getPropertiesSchema()->getItems();
        std::vector<cpp2::RowValue> rows;
        for (auto &item : items) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(item.name_);
            row[1].set_str(columnTypeToString(item.type_));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));
    } while (false);

    DCHECK(onFinish_);
    onFinish_();
}


void DescribeTagExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace vgraph
}   // namespace vesoft
