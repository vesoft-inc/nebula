/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "process/ProcessUtils.h"

namespace nebula {

using nebula::ProcessUtils;

bool getUrl(const std::string& urlPath, std::string& respBody) {
    auto url = folly::stringPrintf("http://%s:%d%s",
                                   FLAGS_ws_ip.c_str(),
                                   FLAGS_ws_http_port,
                                   urlPath.c_str());
    VLOG(1) << "Retrieving url: " << url;

    auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\" 2> /dev/null",
                                       url.c_str());
    auto result = ProcessUtils::runCommand(command.c_str());
    if (!result.ok()) {
        LOG(ERROR) << "Failed to run curl: " << result.status();
        return false;
    }
    respBody = result.value();
    return true;
}

}  // namespace nebula

