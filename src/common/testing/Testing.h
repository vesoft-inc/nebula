/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TESTING_TESTING_H_
#define COMMON_TESTING_TESTING_H_

#include "common/testing/ServerContext.h"


namespace nebula {
namespace testing {

static auto portPlusOneIsUsable = [] (const auto &portsInUse, auto port) {
    return portsInUse.count(port + 1) == 0UL;
};

}   // namespace testing
}   // namespace nebula

#endif  // COMMON_TESTING_TESTING_H_
