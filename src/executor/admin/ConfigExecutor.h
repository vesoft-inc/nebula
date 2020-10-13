/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_CONFIGEXECUTOR_H_
#define EXECUTOR_ADMIN_CONFIGEXECUTOR_H_

#include "executor/Executor.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

class ConfigBaseExecutor : public Executor {
public:
    ConfigBaseExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

protected:
    std::vector<Value> generateColumns(const meta::cpp2::ConfigItem &item);
    DataSet generateResult(const std::vector<meta::cpp2::ConfigItem> &items);
};

class ShowConfigsExecutor final : public ConfigBaseExecutor {
public:
    ShowConfigsExecutor(const PlanNode *node, QueryContext *qctx)
        : ConfigBaseExecutor("ShowConfigsExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class SetConfigExecutor final : public ConfigBaseExecutor {
public:
    SetConfigExecutor(const PlanNode *node, QueryContext *qctx)
        : ConfigBaseExecutor("SetConfigExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    std::string                              name_;
    Value                                    value_;
};

class GetConfigExecutor final : public ConfigBaseExecutor {
public:
    GetConfigExecutor(const PlanNode *node, QueryContext *qctx)
        : ConfigBaseExecutor("GetConfigExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    std::string                              name_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_CONFIGEXECUTOR_H_
