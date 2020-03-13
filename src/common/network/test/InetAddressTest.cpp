/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "network/InetAddress.h"

namespace nebula {
namespace network {

TEST(InetAddress, build) {
    ASSERT_NO_THROW({
        auto addr = InetAddress("127.2.0.1", 20);
        ASSERT_EQ(addr.getAddressStr(), "127.2.0.1");
        ASSERT_EQ(addr.getPort(), 20);
    });
    ASSERT_NO_THROW({
        auto addr = InetAddress("192.168.0.1", 20);
        auto addr2 = InetAddress(addr.toLong(), 20);
        ASSERT_EQ(addr, addr2);
    });

    ASSERT_ANY_THROW({ auto addr = InetAddress("a.b.c.d", 100); });

    ASSERT_NO_THROW({
        auto addr = InetAddress("localhost", 20, true);
        ASSERT_EQ(addr.getPort(), 20);
        ASSERT_EQ(addr.getHostStr(), "localhost");
    });
}
}   // namespace network
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
