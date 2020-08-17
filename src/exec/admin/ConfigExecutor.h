/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_CONFIGEXECUTOR_H_
#define EXEC_ADMIN_CONFIGEXECUTOR_H_

#include "exec/Executor.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

class ConfigBaseExecutor : public Executor {
public:
    ConfigBaseExecutor(const std::string &name, const PlanNode *node, QueryContext *ectx)
        : Executor(name, node, ectx) {}

protected:
    std::vector<Value> generateColumns(const meta::cpp2::ConfigItem &item);
    DataSet generateResult(const std::vector<meta::cpp2::ConfigItem> &items);
};

class ShowConfigsExecutor final : public ConfigBaseExecutor {
public:
    ShowConfigsExecutor(const PlanNode *node, QueryContext *ectx)
        : ConfigBaseExecutor("ShowConfigsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class SetConfigExecutor final : public ConfigBaseExecutor {
public:
    SetConfigExecutor(const PlanNode *node, QueryContext *ectx)
        : ConfigBaseExecutor("SetConfigExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    std::string                              name_;
    Value                                    value_;
};

class GetConfigExecutor final : public ConfigBaseExecutor {
public:
    GetConfigExecutor(const PlanNode *node, QueryContext *ectx)
        : ConfigBaseExecutor("GetConfigExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    std::string                              name_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_CONFIGEXECUTOR_H_
