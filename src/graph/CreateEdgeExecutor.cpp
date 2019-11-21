/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/CreateEdgeExecutor.h"
#include "dataman/ResultSchemaProvider.h"
#include "graph/SchemaHelper.h"

namespace nebula {
namespace graph {

CreateEdgeExecutor::CreateEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateEdgeSentence*>(sentence);
}


Status CreateEdgeExecutor::prepare() {
    return Status::OK();
}


Status CreateEdgeExecutor::getSchema() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        return status;
    }

    const auto& specs = sentence_->columnSpecs();
    const auto& schemaProps = sentence_->getSchemaProps();

    return SchemaHelper::createSchema(specs, schemaProps, schema_);
}


void CreateEdgeExecutor::execute() {
    auto status = getSchema();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->createEdgeSchema(spaceId, *name, schema_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
