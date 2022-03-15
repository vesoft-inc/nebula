/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/VerifyClientVersionProcessor.h"

#include "version/Version.h"

DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":3.0.0",
              "A white list for different client versions, separate with colon.");

namespace nebula {
namespace meta {
void VerifyClientVersionProcessor::process(const cpp2::VerifyClientVersionReq& req) {
  std::unordered_set<std::string> whiteList;
  folly::splitTo<std::string>(
      ":", FLAGS_client_white_list, std::inserter(whiteList, whiteList.begin()));
  if (FLAGS_enable_client_white_list &&
      whiteList.find(req.get_client_version()) == whiteList.end()) {
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE;
    resp_.error_msg_ref() = folly::stringPrintf(
        "Meta client version(%s) is not accepted, current meta client white list: %s.",
        req.get_client_version().c_str(),
        FLAGS_client_white_list.c_str());
  } else {
    const auto& host = req.get_host();
    auto versionKey = MetaKeyUtils::versionKey(host);
    auto versionVal = MetaKeyUtils::versionVal(req.get_build_version().c_str());
    std::vector<kvstore::KV> versionData;
    versionData.emplace_back(std::move(versionKey), std::move(versionVal));
    doSyncPut(versionData);
    resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  onFinished();
}
}  // namespace meta
}  // namespace nebula
