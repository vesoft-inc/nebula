/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/CreateEdgeExecutor.h"
#include "dataman/ResultSchemaProvider.h"

namespace nebula {
namespace graph {

CreateEdgeExecutor::CreateEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateEdgeSentence*>(sentence);
}


Status CreateEdgeExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void CreateEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    const auto& specs = sentence_->columnSpecs();
    auto space = ectx()->rctx()->session()->space();

    nebula::cpp2::Schema schema;
    for (auto& spec : specs) {
        nebula::cpp2::ColumnDef column;
        column.name = *spec->name();
        column.type.type = columnTypeToSupportedType(spec->type());
        schema.columns.emplace_back(std::move(column));
    }

    auto future = mc->addEdgeSchema(space, *name, schema);
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

}   // namespace graph
}   // namespace nebula
