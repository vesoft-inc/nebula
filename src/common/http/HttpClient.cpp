/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "process/ProcessUtils.h"

namespace nebula {
namespace http {

StatusOr<std::string> HttpClient::get(const std::string& path, const std::string& options) {
    auto command = folly::stringPrintf("/usr/bin/curl %s \"%s\"", options.c_str(), path.c_str());
    LOG(INFO) << "HTTP Start Get Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());

    if (result.ok()) {
        LOG(INFO) << "HTTP Get Finished: " << command << " : " << result.value();
        return result;
    } else {
        LOG(INFO) << "HTTP Get Failed: " << command << " : " << result.status().toString();
        return Status::Error(folly::stringPrintf("Http Get Failed: %s", path.c_str()));
    }
}

StatusOr<std::string> HttpClient::post(const std::string& path, const std::string& header) {
    auto command = folly::stringPrintf("/usr/bin/curl -X POST %s \"%s\"",
                                       header.c_str(),
                                       path.c_str());
    LOG(INFO) << "HTTP Start Post Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    if (result.ok()) {
        LOG(INFO) << "HTTP Post Finished: " << command << " : " << result.value();
        return result;
    } else {
        LOG(INFO) << "HTTP Post Failed: " << command << " : " << result.status().toString();
        return Status::Error(folly::stringPrintf("Http Post Failed: %s", path.c_str()));
    }
}

}   // namespace http
}   // namespace nebula
