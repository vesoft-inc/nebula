/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

namespace nebula {

std::ostream& operator <<(std::ostream &os, const HostAddr &addr) {
    uint32_t ip = addr.first;
    uint32_t port = addr.second;
    os << folly::stringPrintf("[%u.%u.%u.%u:%u]",
                              (ip >> 24) & 0xFF,
                              (ip >> 16) & 0xFF,
                              (ip >> 8) & 0xFF,
                              ip & 0xFF, port);
    return os;
}


std::string versionString() {
#if defined(GIT_INFO_SHA)
    return folly::stringPrintf("Git: %s, Build Time: %s %s",
                                NEBULA_STRINGIFY(GIT_INFO_SHA),
                                __DATE__, __TIME__);
#else
    return folly::stringPrintf("Build Time: %s %s", __DATE__, __TIME__);
#endif
}

}   // namespace nebula
