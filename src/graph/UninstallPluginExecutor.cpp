/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/UninstallPluginExecutor.h"

namespace nebula {
namespace graph {

UninstallPluginExecutor::UninstallPluginExecutor(Sentence *sentence,
                                                 ExecutionContext *ectx)
    : Executor(ectx, "uninstall_plugin") {
    sentence_ = static_cast<UninstallPluginSentence*>(sentence);
}

Status UninstallPluginExecutor::prepare() {
    return Status::OK();
}

void UninstallPluginExecutor::execute() {
    pluginName_ = sentence_->pluginName();
    auto future = ectx()->getMetaClient()->uninstallPlugin(*pluginName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Uninstall plugin %s failed: %s",
                                  pluginName_->c_str(),
                                  resp.status().toString().c_str()));
            return;
        }

        auto ret = std::move(resp).value();
        if (!ret) {
            doError(Status::Error("Uninstall plugin %s failed",
                                  pluginName_->c_str()));
            return;
        }

        // close so file
        ectx()->getPluginManager()->close(*pluginName_);

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Uninstall plugin %s exception: %s",
                                       pluginName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
