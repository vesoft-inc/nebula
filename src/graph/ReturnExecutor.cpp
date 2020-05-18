/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ReturnExecutor.h"
#include "graph/InterimResult.h"

namespace nebula {
namespace graph {
ReturnExecutor::ReturnExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx, "return") {
    sentence_ = static_cast<ReturnSentence*>(sentence);
}

Status ReturnExecutor::prepare() {
    return Status::OK();
}

void ReturnExecutor::execute() {
    DCHECK(sentence_);

    auto *var = sentence_->var();
    auto *condition = sentence_->condition();
    if (var == nullptr) {
        // Shall never reach here.
        doError(Status::SyntaxError("Variable not declared."));
        return;
    }

    if ((condition != nullptr) && (*condition != *var)) {
        doError(Status::SyntaxError(
                    "Variable(%s) to be returned is not euqal to condition(%s)",
                    var->c_str(), condition->c_str()));
        return;
    }

    bool existing = false;
    auto *varInputs = ectx()->variableHolder()->get(*var, &existing);
    if (varInputs == nullptr && !existing) {
        doError(Status::Error("Variable(%s) not declared.", var->c_str()));
        return;
    }

    if (varInputs == nullptr || !varInputs->hasData()) {
        doFinish(Executor::ProcessControl::kNext);
        return;
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto colNames = varInputs->getColNames();
        resp_->set_column_names(std::move(colNames));
        auto ret = varInputs->getRows();
        if (!ret.ok()) {
            LOG(ERROR) << "Get rows failed: " << ret.status();
            doError(std::move(ret).status());
            return;
        }
        auto rows = ret.value();
        if (rows.empty()) {
            doFinish(Executor::ProcessControl::kNext);
            return;
        }
        resp_->set_rows(std::move(rows));
        // Will return if variable has values.
        doFinish(Executor::ProcessControl::kReturn);
        return;
    }
}

void ReturnExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}
}  // namespace graph
}  // namespace nebula
