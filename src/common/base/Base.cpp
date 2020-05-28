/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

namespace nebula {

std::string versionString() {
    std::string version;
#if defined(NEBULA_BUILD_VERSION)
    version = folly::stringPrintf("%s, ", NEBULA_STRINGIFY(NEBULA_BUILD_VERSION));
#endif

#if defined(GIT_INFO_SHA)
    version += folly::stringPrintf("Git: %s", NEBULA_STRINGIFY(GIT_INFO_SHA));
#endif
    version += folly::stringPrintf(", Build Time: %s %s", __DATE__, __TIME__);
    version += "\nThis source code is licensed under Apache 2.0 License,"
               " attached with Common Clause Condition 1.0.";
    return version;
}

}   // namespace nebula
