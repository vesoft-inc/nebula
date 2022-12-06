/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "common/http/HttpClient.h"
#include "webservice/WebService.h"

namespace nebula {

bool getUrl(const std::string& urlPath, std::string& respBody) {
  auto url = folly::stringPrintf(
      "http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, urlPath.c_str());
  VLOG(1) << "Retrieving url: " << url;

  auto httpResp = HttpClient::instance().get(url.c_str());
  if (httpResp.curlCode != 0) {
    std::string error = fmt::format("curl error({}): {}", httpResp.curlCode, httpResp.curlMessage);
    LOG(ERROR) << error;
    return false;
  }

  respBody = httpResp.body;
  return true;
}

StatusOr<std::string> putUrl(const std::string& urlPath, const folly::dynamic& data) {
  auto url = folly::stringPrintf(
      "http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, urlPath.c_str());
  VLOG(1) << "Retrieving url: " << url;

  auto httpResp = HttpClient::instance().put(url.c_str(), {}, folly::toJson(data));
  if (httpResp.curlCode != 0) {
    std::string error = fmt::format("curl error({}): {}", httpResp.curlCode, httpResp.curlMessage);
    LOG(ERROR) << error;
    return Status::Error(error);
  }
  return httpResp.body;
}

}  // namespace nebula
