/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_TEST_TESTBASE_H_
#define MOCK_TEST_TESTBASE_H_

#include "base/Base.h"
#include <gtest/gtest.h>
#include "gen-cpp2/GraphService.h"

namespace nebula {
namespace graph {

class TestBase : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;
};

}   // namespace graph
}   // namespace nebula

#endif  // MOCK_TEST_TESTBASE_H_
