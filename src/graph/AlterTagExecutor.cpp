/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/AlterTagExecutor.h"

namespace nebula {
namespace graph {

AlterTagExecutor::AlterTagExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterTagSentence*>(sentence);
}


Status AlterTagExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void AlterTagExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    const auto& tagOpts = sentence_->tagOptList();
    auto spaceId = ectx()->rctx()->session()->space();

    std::vector<nebula::meta::cpp2::AlterTagItem> tagItems;
    for (auto& tagOpt : tagOpts) {
        nebula::meta::cpp2::AlterTagItem tagItem;
        auto opType = getTagOpType(tagOpt->getOptType());
        tagItem.set_op(std::move(opType));
        const auto& specs = tagOpt->columnSpecs();
        nebula::cpp2::Schema schema;
        for (auto& spec : specs) {
            nebula::cpp2::ColumnDef column;
            column.name = *spec->name();
            column.type.type = columnTypeToSupportedType(spec->type());
            schema.columns.emplace_back(std::move(column));
        }
        tagItem.set_schema(std::move(schema));
        tagItems.emplace_back(std::move(tagItem));
    }

    auto future = mc->alterTagSchema(spaceId, *name, tagItems);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

nebula::meta::cpp2::AlterTagOp
AlterTagExecutor::getTagOpType(const AlterTagOptItem::OptionType type) {
    switch (type) {
        case AlterTagOptItem::OptionType::ADD :
            return nebula::meta::cpp2::AlterTagOp::ADD;
        case AlterTagOptItem::OptionType::SET :
            return nebula::meta::cpp2::AlterTagOp::SET;
        case AlterTagOptItem::OptionType::DROP :
            return nebula::meta::cpp2::AlterTagOp::DROP;
        default:
            return nebula::meta::cpp2::AlterTagOp::UNKNOWN;
    }
}
}   // namespace graph
}   // namespace nebula
