/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "process/ProcessUtils.h"

namespace nebula {
namespace http {

StatusOr<std::string> HttpClient::get(const std::string& path) {
    auto command = folly::stringPrintf("curl -sG \"%s\"", path.c_str());
    LOG(INFO) << "HTTP Get Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Get Failed: %s", path.c_str()));
    }
}

// static
StatusOr<std::string> HttpClient::put(const std::string& path, const folly::dynamic& data) {
    std::string command;
    if (data.empty()) {
        command = folly::stringPrintf("curl -X PUT -s \"%s\"", path.c_str());
    } else {
        command = folly::stringPrintf(
            "curl -X PUT -H \"Content-Type: application/json\" -d'%s' -s \"%s\"",
            folly::toJson(data).c_str(),
            path.c_str());
    }
    LOG(INFO) << "HTTP PUT Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Put Failed: %s", path.c_str()));
    }
}

// static
StatusOr<std::string> HttpClient::post(const std::string& path, const folly::dynamic& data) {
    std::string command;
    if (data.empty()) {
        command = folly::stringPrintf("curl -X POST -s \"%s\"", path.c_str());
    } else {
        command = folly::stringPrintf(
            "curl -X POST -H \"Content-Type: application/json\" -d'%s' -s \"%s\"",
            folly::toJson(data).c_str(),
            path.c_str());
    }
    LOG(INFO) << "HTTP POST Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Post Failed: %s", path.c_str()));
    }
}

}   // namespace http
}   // namespace nebula
