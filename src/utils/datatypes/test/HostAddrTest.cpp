/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/datatypes/HostAddr.h"

namespace nebula {

TEST(HostAddr, HashTest) {
    HostAddr addr1("123.45.67.89", 0xFFEEDDCC);
    HostAddr addr2("123.45.67.89", 0xFFEEDDCC);
    EXPECT_EQ(std::hash<HostAddr>()(addr1), std::hash<HostAddr>()(addr2));

    HostAddr addr3("123.45.67.89", 0x00000001);
    HostAddr addr4("123.45.67.89", 0x00000002);
    EXPECT_NE(std::hash<HostAddr>()(addr3), std::hash<HostAddr>()(addr4));

    HostAddr addr5("123.45.67.89", 0x00000001);
    HostAddr addr6("123.45.67.89", 0x00000001);
    EXPECT_EQ(std::hash<HostAddr>()(addr5), std::hash<HostAddr>()(addr6));

    HostAddr addr7("123.45.67.89", 0x10000000);
    HostAddr addr8("123.45.67.89", 0x20000000);
    EXPECT_NE(std::hash<HostAddr>()(addr7), std::hash<HostAddr>()(addr8));

    HostAddr addr9("123.45.67.89", 0x10000000);
    HostAddr addr10("123.45.67.89", 0x10000000);
    EXPECT_EQ(std::hash<HostAddr>()(addr9), std::hash<HostAddr>()(addr10));
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
