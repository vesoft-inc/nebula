/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/CreateTagExecutor.h"
#include "dataman/ResultSchemaProvider.h"

namespace nebula {
namespace graph {

CreateTagExecutor::CreateTagExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateTagSentence*>(sentence);
}


Status CreateTagExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void CreateTagExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    const auto& specs = sentence_->columnSpecs();
    auto spaceId = ectx()->rctx()->session()->space();

    nebula::cpp2::Schema schema;
    for (auto& spec : specs) {
        nebula::cpp2::ColumnDef column;
        column.name = *spec->name();
        column.type.type = columnTypeToSupportedType(spec->type());
        schema.columns.emplace_back(std::move(column));
    }

    auto future = mc->createTagSchema(spaceId, *name, schema);
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
