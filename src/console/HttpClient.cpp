/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "http/HttpClient.h"
#include "process/ProcessUtils.h"
#include<time.h>
#include<iostream>
namespace nebula {
namespace http {

StatusOr<std::string> HttpClient::get(const std::string& path) {
    auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\"", path.c_str());
    LOG(INFO) << "HTTP Get Command: " << command;
    auto result = nebula::ProcessUtils::runCommand(command.c_str());
    time_t now_time = time(NULL);
    tm*  t_tm = localtime_r(&now_time);
    if (result.ok()) {
        std:: cout << "local time is    " << asctime_r(t_tm);
        return result.value();
    } else {
        std:: cout << "local time is    " << asctime_r(t_tm);
        return Status::Error(folly::stringPrintf("Http Get Failed: %s", path.c_str()));
    }
}

}   // namespace http
}   // namespace nebula
