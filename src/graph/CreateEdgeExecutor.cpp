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
                                       ExecutionContext *ectx)
    : Executor(ectx, "create_edge") {
    exprCtx_ = std::make_unique<ExpressionContext>();
    sentence_ = static_cast<CreateEdgeSentence*>(sentence);
}


Status CreateEdgeExecutor::prepare() {
    for (auto spec : sentence_->columnSpecs()) {
        spec->setContext(exprCtx_.get());
    }
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
        doError(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->createEdgeSchema(spaceId, *name, schema_, sentence_->isIfNotExist());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Create edge `%s' failed: %s.",
                        sentence_->name()->c_str(), resp.status().toString().c_str()));
            return;
        }

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Create edge `%s' exception: %s",
                sentence_->name()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
