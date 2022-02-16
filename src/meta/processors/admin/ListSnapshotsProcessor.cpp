/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/ListSnapshotsProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <memory>     // for unique_ptr
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for basic_string, operator<<
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPrefix

namespace nebula {
namespace meta {

void ListSnapshotsProcessor::process(const cpp2::ListSnapshotsReq&) {
  const auto& prefix = MetaKeyUtils::snapshotPrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Snapshot prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
    snapshot.name_ref() = std::move(name);
    snapshot.status_ref() = std::move(status);
    snapshot.hosts_ref() = std::move(hosts);
    snapshots.emplace_back(std::move(snapshot));
    iter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.snapshots_ref() = std::move(snapshots);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
