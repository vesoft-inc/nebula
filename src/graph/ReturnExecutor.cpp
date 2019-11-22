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
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ReturnSentence*>(sentence);
}

Status ReturnExecutor::prepare() {
    return Status::OK();
}

void ReturnExecutor::execute() {
    FLOG_INFO("Executing Return: %s", sentence_->toString().c_str());
    DCHECK(onFinish_);
    DCHECK(onError_);
    DCHECK(sentence_);

    auto *var = sentence_->var();
    auto *condition = sentence_->condition();
    if (var == nullptr) {
        // Shall never reach here.
        onError_(Status::SyntaxError("Variable not declared."));
        return;
    }

    if ((condition != nullptr) && (*condition != *var)) {
        onError_(Status::SyntaxError(
                    "Variable(%s) to be returned is not euqal to condition(%s)", var, condition));
        return;
    }

    bool existing = false;
    auto *varInputs = ectx()->variableHolder()->get(*var, &existing);
    if (varInputs == nullptr && !existing) {
        onError_(Status::Error("Variable(%s) not declared.", var));
        return;
    }

    if (varInputs == nullptr || !varInputs->hasData()) {
        onFinish_(Executor::ProcessControl::kNext);
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        auto colNames = varInputs->getColNames();
        resp_->set_column_names(std::move(colNames));
        auto ret = varInputs->getRows();
        if (!ret.ok()) {
            LOG(ERROR) << "Get rows failed: " << ret.status();
            onError_(std::move(ret).status());
            return;
        }
        auto rows = ret.value();
        if (rows.empty()) {
            onFinish_(Executor::ProcessControl::kNext);
            return;
        }
        resp_->set_rows(std::move(rows));
        // Will return if variable has values.
        onFinish_(Executor::ProcessControl::kReturn);
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
