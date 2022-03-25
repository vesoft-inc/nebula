/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/CloudAuthenticator.h"

#include <proxygen/lib/utils/CryptUtil.h>

#include "common/base/Status.h"
#include "common/http/HttpClient.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

CloudAuthenticator::CloudAuthenticator(meta::MetaClient* client) {
  metaClient_ = client;
}

Status CloudAuthenticator::auth(const std::string& user, const std::string& password) {
  // The shadow account on the nebula side has been created
  // Normal passwords and tokens use different prefixes

  // First, go to meta to check if the shadow account exists
  if (!metaClient_->checkShadowAccountFromCache(user)) {
    return Status::Error("Shadow account not exist");
  }

  // Second, use user + password authentication methods
  std::string userAndPasswd = user + ":" + password;
  std::string base64Str = proxygen::base64Encode(folly::StringPiece(userAndPasswd));

  std::string header = "-H \"Content-Type: application/json\"  -H \"Authorization:Nebula ";
  header = header + base64Str + "\"";
  auto result = http::HttpClient::post(FLAGS_cloud_http_url, header);

  if (!result.ok()) {
    LOG(ERROR) << result.status();
    return result.status();
  }

  try {
    auto json = folly::parseJson(result.value());
    if (json["code"].asString().compare("0") != 0) {
      LOG(ERROR) << "Cloud authentication failed, user: " << user;
      return Status::Error("Cloud authentication failed, user: %s", user.c_str());
    }
  } catch (std::exception& e) {
    LOG(ERROR) << "Invalid json: " << e.what();
    return Status::Error("Invalid json: %s", e.what());
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
