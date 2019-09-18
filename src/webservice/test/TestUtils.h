/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/WebService.h"
#include "http/HttpClient.h"

namespace nebula {

bool getUrl(const std::string& urlPath, std::string& respBody) {
    auto url = folly::stringPrintf("http://%s:%d%s",
                                   FLAGS_ws_ip.c_str(),
                                   FLAGS_ws_http_port,
                                   urlPath.c_str());
    VLOG(1) << "Retrieving url: " << url;

    auto result = http::HttpClient::get(url.c_str());
    if (!result.ok()) {
        LOG(ERROR) << "Failed to run curl: " << result.status();
        return false;
    }
    respBody = result.value();
    return true;
}

}  // namespace nebula

