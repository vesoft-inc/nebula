// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/SnapshotExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateSnapshotExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  return qctx()->getMetaClient()->createSnapshot().via(runner()).thenValue([](StatusOr<bool> resp) {
    if (!resp.ok()) {
      LOG(WARNING) << "Create snapshot fail: " << resp.status();
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
          LOG(WARNING) << "Drop snapshot fail: " << resp.status();
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
          LOG(WARNING) << "Show snapshot fail: " << resp.status();
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
