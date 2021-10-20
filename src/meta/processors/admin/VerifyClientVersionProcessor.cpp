/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/VerifyClientVersionProcessor.h"

#include "version/Version.h"

DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":2.5.0:2.5.1:2.6.0",
              "A white list for different client versions, seperate with colon.");

namespace nebula {
namespace meta {
void VerifyClientVersionProcessor::process(const cpp2::VerifyClientVersionReq& req) {
  std::unordered_set<std::string> whiteList;
  folly::splitTo<std::string>(
      ":", FLAGS_client_white_list, std::inserter(whiteList, whiteList.begin()));
  if (FLAGS_enable_client_white_list && whiteList.find(req.get_version()) == whiteList.end()) {
    resp_.set_code(nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE);
    resp_.set_error_msg(folly::stringPrintf(
        "Meta client version(%s) is not accepted, current meta client white list: %s.",
        req.get_version().c_str(),
        FLAGS_client_white_list.c_str()));
  } else {
    resp_.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
  }
  onFinished();
}
}  // namespace meta
}  // namespace nebula
