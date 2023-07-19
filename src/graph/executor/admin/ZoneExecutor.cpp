// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/ZoneExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> MergeZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *mzNode = asNode<MergeZone>(node());
  return qctx()
      ->getMetaClient()
      ->mergeZone(mzNode->zones(), mzNode->zoneName())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Merge Zone Failed :" << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> RenameZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *rzNode = asNode<RenameZone>(node());
  return qctx()
      ->getMetaClient()
      ->renameZone(rzNode->originalZoneName(), rzNode->zoneName())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Rename Zone Failed :" << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DropZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *dzNode = asNode<DropZone>(node());
  return qctx()
      ->getMetaClient()
      ->dropZone(dzNode->zoneName())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Drop Zone Failed :" << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DivideZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *dzNode = asNode<DivideZone>(node());
  return qctx()
      ->getMetaClient()
      ->divideZone(dzNode->zoneName(), dzNode->zoneItems())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Split Zone Failed :" << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DescribeZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *dzNode = asNode<DescribeZone>(node());
  return qctx()
      ->getMetaClient()
      ->getZone(dzNode->zoneName())
      .via(runner())
      .thenValue([this](StatusOr<std::vector<HostAddr>> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Describe Zone Failed: " << resp.status();
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
                          .build());
      });
}

folly::Future<Status> AddHostsIntoZoneExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *ahNode = asNode<AddHostsIntoZone>(node());
  return qctx()
      ->getMetaClient()
      ->addHostsIntoZone(ahNode->address(), ahNode->zoneName(), ahNode->isNew())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Add Host Into Zone Failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> ListZonesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return qctx()->getMetaClient()->listZones().via(runner()).thenValue(
      [this](StatusOr<std::vector<meta::cpp2::Zone>> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "List Zones Failed: " << resp.status();
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
                          .build());
      });
}

}  // namespace graph
}  // namespace nebula
