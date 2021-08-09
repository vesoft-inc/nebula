/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/ZoneExecutor.h"
#include "planner/plan/Admin.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> AddZoneExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *azNode = asNode<AddZone>(node());
    return qctx()->getMetaClient()->addZone(azNode->zoneName(), azNode->addresses())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Add Zone Failed :" << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropZoneExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dzNode = asNode<DropZone>(node());
    return qctx()->getMetaClient()->dropZone(dzNode->zoneName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop Zone Failed :" << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DescribeZoneExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dzNode = asNode<DescribeZone>(node());
    return qctx()->getMetaClient()->getZone(dzNode->zoneName())
            .via(runner())
            .thenValue([this](StatusOr<std::vector<HostAddr>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Describe Zone Failed: " << resp.status();
                    return resp.status();
                }

                auto hosts = std::move(resp).value();
                DataSet dataSet({"Hosts", "Port"});
                for (auto &host : hosts) {
                    Row row({host.host, host.port});
                    dataSet.rows.emplace_back(std::move(row));
                }

                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<Status> AddHostIntoZoneExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *ahNode = asNode<AddHostIntoZone>(node());
    return qctx()->getMetaClient()->addHostIntoZone(ahNode->address(), ahNode->zoneName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Add Host Into Zone Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropHostFromZoneExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *dhNode = asNode<DropHostFromZone>(node());
    return qctx()->getMetaClient()->dropHostFromZone(dhNode->address(), dhNode->zoneName())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop Host From Zone Failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ListZonesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return qctx()->getMetaClient()->listZones()
            .via(runner())
            .thenValue([this](StatusOr<std::vector<meta::cpp2::Zone>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "List Zones Failed: " << resp.status();
                    return resp.status();
                }

                auto zones = std::move(resp).value();
                DataSet dataSet({"Name", "Host", "Port"});
                for (auto &zone : zones) {
                    for (auto &host : zone.get_nodes()) {
                        Row row({*zone.zone_name_ref(), host.host, host.port});
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
