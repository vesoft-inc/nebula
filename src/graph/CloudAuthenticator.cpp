/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CloudAuthenticator.h"
#include "encryption/Base64.h"
#include "http/HttpClient.h"
#include "graph/GraphFlags.h"
#include <folly/json.h>

namespace nebula {
namespace graph {

CloudAuthenticator::CloudAuthenticator(meta::MetaClient* client) {
    metaClient_ = client;
}

bool CloudAuthenticator::auth(const std::string& user, const std::string& password) {
    // The shadow account on the nebula side has been created
    // Normal passwords and tokens use different prefixes

    // First, go to meta to check if the shadow account exists
    if (!metaClient_->checkShadowAccountFromCache(user)) {
        return false;
    }

    // Second, use user + password authentication methods
    std::string userAndPasswd = user + ":" + password;
    std::string base64Str = encryption::Base64::encode(userAndPasswd);

    std::string header = "-H \"Content-Type: application/json\"  -H \"Authorization:Nebula ";
    header =  header + base64Str + "\"";
    auto result = http::HttpClient::post(FLAGS_cloud_http_url, header);

    if (!result.ok()) {
        LOG(ERROR) << result.status();
        return false;
    }

    try {
        auto json = folly::parseJson(result.value());
        if (json["code"].asString().compare("0") != 0) {
            LOG(ERROR) << "Cloud authentication failed";
            return false;
        }
    } catch (std::exception& e) {
        LOG(ERROR) << "Invalid json: " << e.what();
        return false;
    }
    return true;
}

}   // namespace graph
}   // namespace nebula
