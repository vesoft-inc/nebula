/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AlterEdgeExecutor.h"

namespace nebula {
namespace graph {

AlterEdgeExecutor::AlterEdgeExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterEdgeSentence*>(sentence);
}


Status AlterEdgeExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void AlterEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    const auto& edgeOpts = sentence_->edgeOptList();
    auto spaceId = ectx()->rctx()->session()->space();

    std::vector<nebula::meta::cpp2::AlterSchemaItem> edgeItems;
    for (auto& edgeOpt : edgeOpts) {
        nebula::meta::cpp2::AlterSchemaItem edgeItem;
        auto opType = getEdgeOpType(edgeOpt->getOptType());
        edgeItem.set_op(std::move(opType));
        const auto& specs = edgeOpt->columnSpecs();
        nebula::cpp2::Schema schema;
        for (auto& spec : specs) {
            nebula::cpp2::ColumnDef column;
            column.name = *spec->name();
            column.type.type = columnTypeToSupportedType(spec->type());
            schema.columns.emplace_back(std::move(column));
        }
        edgeItem.set_schema(std::move(schema));
        edgeItems.emplace_back(std::move(edgeItem));
    }

    auto future = mc->alterEdge(spaceId, *name, std::move(edgeItems));
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


nebula::meta::cpp2::AlterSchemaOp
AlterEdgeExecutor::getEdgeOpType(const AlterEdgeOptItem::OptionType type) {
    switch (type) {
        case AlterEdgeOptItem::OptionType::ADD :
            return nebula::meta::cpp2::AlterSchemaOp::ADD;
        case AlterEdgeOptItem::OptionType::CHANGE :
            return nebula::meta::cpp2::AlterSchemaOp::CHANGE;
        case AlterEdgeOptItem::OptionType::DROP :
            return nebula::meta::cpp2::AlterSchemaOp::DROP;
        default:
            return nebula::meta::cpp2::AlterSchemaOp::UNKNOWN;
    }
}

}   // namespace graph
}   // namespace nebula
