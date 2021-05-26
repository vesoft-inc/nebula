/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestBase.h"

namespace nebula {
namespace graph {

void TestBase::SetUp() {
    time_t curTime = time(0);
    timezoneOffset_ = timegm(localtime(&curTime)) - curTime;
}


void TestBase::TearDown() {
}

}   // namespace graph
}   // namespace nebula
