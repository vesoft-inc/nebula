/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ZoneExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Future.h>      // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>, Prom...
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>    // for max
#include <ostream>      // for operator<<, basic_ostream
#include <string>       // for string, basic_string, all...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for operator<<, Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Admin.h"       // for AddHostsIntoZone, DivideZone
#include "interface/gen-cpp2/meta_types.h"  // for Zone

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
          LOG(ERROR) << "Merge Zone Failed :" << resp.status();
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
          LOG(ERROR) << "Rename Zone Failed :" << resp.status();
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
          LOG(ERROR) << "Drop Zone Failed :" << resp.status();
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
          LOG(ERROR) << "Split Zone Failed :" << resp.status();
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
          LOG(ERROR) << "Add Host Into Zone Failed: " << resp.status();
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
                          .build());
      });
}

}  // namespace graph
}  // namespace nebula
