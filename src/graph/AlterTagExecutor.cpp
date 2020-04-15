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
            doError(Status::Error("Alter tag `%s' failed: %s",
                        sentence_->name()->c_str(), resp.status().toString().c_str()));
            return;
        }

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Alter tag `%s' exception: %s.",
                sentence_->name()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
