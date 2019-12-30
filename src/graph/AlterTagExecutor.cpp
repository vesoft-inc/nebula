/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AlterTagExecutor.h"
#include "graph/SchemaHelper.h"

namespace nebula {
namespace graph {

AlterTagExecutor::AlterTagExecutor(Sentence *sentence,
                                   ExecutionContext *ectx)
    : Executor(ectx, "alter_tag") {
    sentence_ = static_cast<AlterTagSentence*>(sentence);
}


Status AlterTagExecutor::prepare() {
    return Status::OK();
}


Status AlterTagExecutor::getSchema() {
    auto status = checkIfGraphSpaceChosen();

    if (!status.ok()) {
        return status;
    }

    const auto& schemaOpts = sentence_->getSchemaOpts();
    const auto& schemaProps = sentence_->getSchemaProps();

    return SchemaHelper::alterSchema(schemaOpts, schemaProps, options_, schemaProp_);
}


void AlterTagExecutor::execute() {
    auto status = getSchema();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->alterTagSchema(spaceId, *name, std::move(options_), std::move(schemaProp_));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(resp.status());
            return;
        }

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
