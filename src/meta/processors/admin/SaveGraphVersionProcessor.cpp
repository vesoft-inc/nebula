/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/SaveGraphVersionProcessor.h"

#include "common/graph/Response.h"
#include "version/Version.h"

namespace nebula {
namespace meta {
void SaveGraphVersionProcessor::process(const cpp2::SaveGraphVersionReq& req) {
  const auto& host = req.get_host();

  // Build a map of graph service host and its version
  auto versionKey = MetaKeyUtils::versionKey(host);
  auto versionVal = MetaKeyUtils::versionVal(req.get_build_version().c_str());
  std::vector<kvstore::KV> versionData;
  versionData.emplace_back(std::move(versionKey), std::move(versionVal));

  // Save the version of the graph service
  auto errCode = doSyncPut(versionData);
  if (errCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Failed to save graph version, errorCode: "
               << apache::thrift::util::enumNameSafe(errCode);
    handleErrorCode(errCode);
    onFinished();
    return;
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);

  onFinished();
}
}  // namespace meta
}  // namespace nebula
