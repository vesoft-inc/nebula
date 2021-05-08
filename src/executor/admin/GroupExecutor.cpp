/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/GroupExecutor.h"
#include "planner/plan/Admin.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> AddGroupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *agNode = asNode<AddGroup>(node());
    return qctx()->getMetaClient()->addGroup(agNode->groupName(), agNode->zoneNames())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Add Group Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropGroupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dgNode = asNode<DropGroup>(node());
    return qctx()->getMetaClient()->dropGroup(dgNode->groupName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop Group Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DescribeGroupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dgNode = asNode<DescribeGroup>(node());
    return qctx()->getMetaClient()->getGroup(dgNode->groupName())
            .via(runner())
            .thenValue([this](StatusOr<std::vector<std::string>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Describe Group Failed: " << resp.status();
                    return resp.status();
                }

                auto zones = std::move(resp).value();
                DataSet dataSet({"Zone"});
                for (auto &zone : zones) {
                    Row row({zone});
                    dataSet.rows.emplace_back(std::move(row));
                }
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<Status> AddZoneIntoGroupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *azNode = asNode<AddZoneIntoGroup>(node());
    return qctx()->getMetaClient()->addZoneIntoGroup(azNode->zoneName(), azNode->groupName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Add Zone Into Group Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropZoneFromGroupExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dzNode = asNode<DropZoneFromGroup>(node());
    return qctx()->getMetaClient()->dropZoneFromGroup(dzNode->zoneName(), dzNode->groupName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop Zone From Group Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ListGroupsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return qctx()->getMetaClient()->listGroups()
            .via(runner())
            .thenValue([this](StatusOr<std::vector<meta::cpp2::Group>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "List Groups Failed: " << resp.status();
                    return resp.status();
                }

                auto groups = std::move(resp).value();
                DataSet dataSet({"Name", "Zone"});
                for (auto &group : groups) {
                    for (auto &zone : group.get_zone_names()) {
                        Row row({*group.group_name_ref(), zone});
                        dataSet.rows.emplace_back(std::move(row));
                    }
                }
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

}   // namespace graph
}   // namespace nebula
