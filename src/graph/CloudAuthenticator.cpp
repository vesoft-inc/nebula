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

    // Second, use different authentication methods based on the password prefix
    // Use user + password method
    StatusOr<std::string> result;
    if (password[0] == 'A') {
        std::string passwd = password.substr(1);
        std::string userAndPasswd = user + ":" + passwd;
        std::string base64Str = encryption::Base64::encode(userAndPasswd);

        std::string header = "-H \"Content-Type: application/json\"  -H \"Authorization:Basic ";
        header =  header + base64Str + "\"";
        result = http::HttpClient::post(FLAGS_cloud_http_url, header);
    } else if (password[0] == 'T') {
        // Use token method
        std::string passwd = password.substr(1);
        std::string header = "-H \"Content-Type: application/json\"  -H \"Authorization:Bearer ";
        header = header + passwd + "\"";
        result = http::HttpClient::post(FLAGS_cloud_http_url, header);
    } else {
        LOG(ERROR) << "Cloud authentication failed, password is incorrect";
        return false;
    }

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
