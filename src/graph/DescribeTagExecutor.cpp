/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/DescribeTagExecutor.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace graph {

DescribeTagExecutor::DescribeTagExecutor(Sentence *sentence,
                                         ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeTagSentence*>(sentence);
}


Status DescribeTagExecutor::prepare() {
    return Status::OK();
}


void DescribeTagExecutor::execute() {
    auto *name = sentence_->name();
    auto space = ectx()->rctx()->session()->space();
    auto tagId = meta::SchemaManager::toTagID(*name);
    auto schema = meta::SchemaManager::getTagSchema(space, tagId);

    resp_ = std::make_unique<cpp2::ExecutionResponse>();

    do {
        if (schema == nullptr) {
            onError_(Status::Error("Schema not found for tag `%s'", name->c_str()));
            return;
        }
        std::vector<std::string> header{"Column", "Type"};
        resp_->set_column_names(std::move(header));
        uint32_t numFields = schema->getNumFields();
        std::vector<cpp2::RowValue> rows;
        for (uint32_t index = 0; index < numFields; index++) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_str(schema->getFieldName(index));
            row[1].set_str(valueTypeToString(schema->getFieldType(index)));
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

}   // namespace graph
}   // namespace nebula
