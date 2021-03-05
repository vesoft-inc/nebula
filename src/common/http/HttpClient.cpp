/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/http/HttpClient.h"
#include "common/process/ProcessUtils.h"

namespace nebula {
namespace http {

StatusOr<std::string> HttpClient::get(const std::string& path, const std::string& options) {
    auto command = folly::stringPrintf("/usr/bin/curl %s \"%s\"", options.c_str(), path.c_str());
    LOG(INFO) << "HTTP Get Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Get Failed: %s", path.c_str()));
    }
}

StatusOr<std::string> HttpClient::post(const std::string& path, const std::string& header) {
    auto command =
        folly::stringPrintf("/usr/bin/curl -X POST %s \"%s\"", header.c_str(), path.c_str());
    LOG(INFO) << "HTTP POST Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Post Failed: %s", path.c_str()));
    }
}

StatusOr<std::string> HttpClient::post(const std::string& path,
                                       const std::unordered_map<std::string, std::string>& header) {
    folly::dynamic mapData = folly::dynamic::object;
    // Build a dynamic object from map
    for (auto const& it : header) {
        mapData[it.first] = it.second;
    }
    return post(path, mapData);
}

StatusOr<std::string> HttpClient::post(const std::string& path, const folly::dynamic& data) {
    return sendRequest(path, data, "POST");
}

StatusOr<std::string> HttpClient::put(const std::string& path,
                                       const std::unordered_map<std::string, std::string>& header) {
    folly::dynamic mapData = folly::dynamic::object;
    // Build a dynamic object from map
    for (auto const& it : header) {
        mapData[it.first] = it.second;
    }
    return put(path, mapData);
}

StatusOr<std::string> HttpClient::put(const std::string& path, const std::string& header) {
    auto command =
        folly::stringPrintf("/usr/bin/curl -X PUT %s \"%s\"", header.c_str(), path.c_str());
    LOG(INFO) << "HTTP PUT Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(folly::stringPrintf("Http Put Failed: %s", path.c_str()));
    }
}

StatusOr<std::string> HttpClient::put(const std::string& path, const folly::dynamic& data) {
    return sendRequest(path, data, "PUT");
}

StatusOr<std::string> HttpClient::sendRequest(const std::string& path,
                                              const folly::dynamic& data,
                                              const std::string& reqType) {
    std::string command;
    if (data.empty()) {
        command = folly::stringPrintf("curl -X %s -s \"%s\"", reqType.c_str(), path.c_str());
    } else {
        command =
            folly::stringPrintf("curl -X %s -H \"Content-Type: application/json\" -d'%s' -s \"%s\"",
                                reqType.c_str(),
                                folly::toJson(data).c_str(),
                                path.c_str());
    }
    LOG(INFO) << folly::stringPrintf("HTTP %s Command: %s", reqType.c_str(), command.c_str());
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        return result.value();
    } else {
        return Status::Error(
            folly::stringPrintf("Http %s Failed: %s", reqType.c_str(), path.c_str()));
    }
}

}   // namespace http
}   // namespace nebula
