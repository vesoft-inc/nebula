/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InstallPluginExecutor.h"
#include <dlfcn.h>

namespace nebula {
namespace graph {

InstallPluginExecutor::InstallPluginExecutor(Sentence *sentence,
                                             ExecutionContext *ectx)
    : Executor(ectx, "install_plugin") {
    sentence_ = static_cast<InstallPluginSentence*>(sentence);
}

Status InstallPluginExecutor::prepare() {
    // Move this part of the operation to meta
    /*
    auto *pluginName = sentence_->pluginName();
    auto *soName = sentence_->soName();

    // Now place the so file that will be installed to the share directory
    // of the installation directory
    using fs::FileUtils;
    auto dir = FileUtils::readLink("/proc/self/exe").value();
    dir = FileUtils::dirname(dir.c_str()) + "/../share";
    std::string sofile = dir + "/" + *soName;

    dlHandle_ = dlopen(sofile, RTLD_NOW);
    if (!dlHandle_) {
        return Status::Error("Install plugin failed: `%s", dlerror());
    }
    */
    return Status::OK();
}

void InstallPluginExecutor::execute() {
    pluginName_ = sentence_->pluginName();
    soName_ = sentence_->soName();
    auto future = ectx()->getMetaClient()->installPlugin(*pluginName_, *soName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Install plugin %s soname \"%s\" failed: %s",
                                  pluginName_->c_str(),
                                  soName_->c_str(),
                                  resp.status().toString().c_str()));
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            doError(Status::Error("Install plugin %s soname \"%s\" failed",
                                  pluginName_->c_str(),
                                  soName_->c_str()));
            return;
        }

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Install plugin %s exception: %s",
                                       pluginName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
