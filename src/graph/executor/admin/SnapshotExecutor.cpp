/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SnapshotExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>    // for max
#include <string>       // for string, basic_string, all...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for operator<<, Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Admin.h"       // for DropSnapshot
#include "interface/gen-cpp2/meta_types.h"  // for Snapshot

namespace nebula {
namespace graph {

folly::Future<Status> CreateSnapshotExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  return qctx()->getMetaClient()->createSnapshot().via(runner()).thenValue([](StatusOr<bool> resp) {
    if (!resp.ok()) {
      LOG(ERROR) << resp.status();
      return resp.status();
    }
    return Status::OK();
  });
}

folly::Future<Status> DropSnapshotExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *dsNode = asNode<DropSnapshot>(node());
  return qctx()
      ->getMetaClient()
      ->dropSnapshot(dsNode->getSnapshotName())
      .via(runner())
      .thenValue([](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> ShowSnapshotsExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  return qctx()->getMetaClient()->listSnapshots().via(runner()).thenValue(
      [this](StatusOr<std::vector<meta::cpp2::Snapshot>> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }

        auto snapshots = std::move(resp).value();
        DataSet dataSet({"Name", "Status", "Hosts"});

        for (auto &snapshot : snapshots) {
          Row row;
          row.values.emplace_back(*snapshot.name_ref());
          row.values.emplace_back(apache::thrift::util::enumNameSafe(*snapshot.status_ref()));
          row.values.emplace_back(*snapshot.hosts_ref());
          dataSet.rows.emplace_back(std::move(row));
        }
        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}
}  // namespace graph
}  // namespace nebula
