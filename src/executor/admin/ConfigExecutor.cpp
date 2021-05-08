/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/conf/Configuration.h"
#include "executor/admin/ConfigExecutor.h"
#include "planner/plan/Admin.h"
#include "util/SchemaUtil.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

std::vector<Value> ConfigBaseExecutor::generateColumns(const meta::cpp2::ConfigItem& item) {
    std::vector<Value> columns;
    columns.resize(5);
    auto value = item.get_value();
    columns[0].setStr(apache::thrift::util::enumNameSafe(item.get_module()));
    columns[1].setStr(item.get_name());
    columns[2].setStr(value.typeName());
    columns[3].setStr(apache::thrift::util::enumNameSafe(item.get_mode()));
    columns[4] = std::move(value);
    return columns;
}

DataSet ConfigBaseExecutor::generateResult(const std::vector<meta::cpp2::ConfigItem> &items) {
    DataSet result;
    result.colNames = {"module", "name", "type", "mode", "value"};
    for (const auto &item : items) {
        auto columns = generateColumns(item);
        result.rows.emplace_back(std::move(columns));
    }
    return result;
}

folly::Future<Status> ShowConfigsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scNode = asNode<ShowConfigs>(node());
    return qctx()->getMetaClient()->listConfigs(scNode->getModule())
            .via(runner())
            .thenValue([this, scNode](StatusOr<std::vector<meta::cpp2::ConfigItem>> resp) {
                if (!resp.ok()) {
                    auto module = apache::thrift::util::enumNameSafe(scNode->getModule());
                    LOG(ERROR) << "Show configs `" << module
                               << "' failed: " << resp.status();
                    return Status::Error("Show config `%s' failed: %s",
                                          module.c_str(),
                                          resp.status().toString().c_str());
                }

                auto result = generateResult(resp.value());
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<Status> SetConfigExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scNode = asNode<SetConfig>(node());
    return qctx()->getMetaClient()->setConfig(scNode->getModule(),
                                              scNode->getName(),
                                              scNode->getValue())
            .via(runner())
            .thenValue([scNode](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Set config `" << scNode->getName()
                               << "' failed: " << resp.status();
                    return Status::Error("Set config `%s' failed: %s",
                                          scNode->getName().c_str(),
                                          resp.status().toString().c_str());
                }
                return Status::OK();
            });
}

folly::Future<Status> GetConfigExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *gcNode = asNode<GetConfig>(node());
    return qctx()->getMetaClient()->getConfig(gcNode->getModule(),
                                              gcNode->getName())
            .via(runner())
            .thenValue([this, gcNode](StatusOr<std::vector<meta::cpp2::ConfigItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Get config `" << gcNode->getName()
                               << "' failed: " << resp.status();
                    return Status::Error("Get config `%s' failed: %s",
                                          gcNode->getName().c_str(),
                                          resp.status().toString().c_str());
                }
                auto result = generateResult(resp.value());
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

}   // namespace graph
}   // namespace nebula
