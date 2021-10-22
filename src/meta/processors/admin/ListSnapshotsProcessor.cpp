/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/ListSnapshotsProcessor.h"

#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

void ListSnapshotsProcessor::process(const cpp2::ListSnapshotsReq&) {
  const auto& prefix = MetaKeyUtils::snapshotPrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "Snapshot prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(iterRet).get();

  std::vector<nebula::meta::cpp2::Snapshot> snapshots;
  while (iter->valid()) {
    auto val = iter->val();
    auto name = MetaKeyUtils::parseSnapshotName(iter->key());
    auto status = MetaKeyUtils::parseSnapshotStatus(val);
    auto hosts = MetaKeyUtils::parseSnapshotHosts(val);
    cpp2::Snapshot snapshot;
    snapshot.set_name(std::move(name));
    snapshot.set_status(std::move(status));
    snapshot.set_hosts(std::move(hosts));
    snapshots.emplace_back(std::move(snapshot));
    iter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_snapshots(std::move(snapshots));
  onFinished();
}

}  // namespace meta
}  // namespace nebula
