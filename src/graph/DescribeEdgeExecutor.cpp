/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DescribeEdgeExecutor.h"

namespace nebula {
namespace graph {

DescribeEdgeExecutor::DescribeEdgeExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeEdgeSentence*>(sentence);
}


Status DescribeEdgeExecutor::prepare() {
    return Status::OK();
}


void DescribeEdgeExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    auto *schema = sm->getEdgeSchema(*name);
    resp_ = std::make_unique<cpp2::ExecutionResponse>();

    do {
        if (schema == nullptr) {
            onError_(Status::Error("Schema not found for edge `%s'", name->c_str()));
            return;
        }
        std::vector<std::string> header{"SrcTag", "DstTag", "Column", "Type"};
        auto &src = schema->srcTag();
        auto &dst = schema->dstTag();
        auto &items = schema->getPropertiesSchema()->getItems();
        std::vector<cpp2::RowValue> rows;
        for (auto &item : items) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(4);
            row[0].set_str(src);
            row[1].set_str(dst);
            row[2].set_str(item.name_);
            row[3].set_str(columnTypeToString(item.type_));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        if (!rows.empty()) {
            resp_->set_column_names(std::move(header));
        } else {     // no columns
            header.resize(header.size() - 2);
            resp_->set_column_names(std::move(header));
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(src);
            row[1].set_str(dst);
            rows.resize(1);
            rows[0].set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));
    } while (false);

    DCHECK(onFinish_);
    onFinish_();
}


void DescribeEdgeExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
