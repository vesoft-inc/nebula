/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "network/NetworkUtils.h"

namespace nebula {
namespace network {

TEST(NetworkUtils, getHostname) {
    std::string hostname = NetworkUtils::getHostname();

    FILE* fp = popen("LD_PRELOAD= hostname | tr -d ['\n']", "r");
    char buffer[256];
    auto numChars = fgets(buffer, sizeof(buffer), fp);
    UNUSED(numChars);
    pclose(fp);
    EXPECT_EQ(std::string(buffer), hostname);
}


TEST(NetworkUtils, getIPv4FromDevice) {
    {
        auto result = NetworkUtils::getIPv4FromDevice("lo");
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ("127.0.0.1", result.value());
    }
    {
        auto result = NetworkUtils::getIPv4FromDevice("any");
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ("0.0.0.0", result.value());
    }
    {
        auto result = NetworkUtils::getIPv4FromDevice("non-existence");
        ASSERT_FALSE(result.ok()) << result.status();
    }
}


TEST(NetworkUtils, listIPv4s) {
    auto result = NetworkUtils::listIPv4s();
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_FALSE(result.value().empty());
    auto found = false;
    for (auto &ip : result.value()) {
        if (ip == "127.0.0.1") {
            found = true;
        }
    }
    ASSERT_TRUE(found);
}


TEST(NetworkUtils, listDeviceAndIPv4s) {
    auto result = NetworkUtils::listDeviceAndIPv4s();
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_FALSE(result.value().empty());
    ASSERT_NE(result.value().end(), result.value().find("lo"));
}


TEST(NetworkUtils, intIPv4Conversion) {
    IPv4 ip;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt("127.0.0.1", ip));
    EXPECT_EQ(NetworkUtils::intToIPv4(ip), "127.0.0.1");

    ip = 0x11223344;
    IPv4 converted;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt(NetworkUtils::intToIPv4(ip), converted));
    EXPECT_EQ(converted, ip);
}


TEST(NetworkUtils, getDynamicPortRange) {
    uint16_t low, high;
    ASSERT_TRUE(NetworkUtils::getDynamicPortRange(low, high));
    ASSERT_NE(low, high);
}


TEST(NetworkUtils, getAvailablePort) {
    auto port = NetworkUtils::getAvailablePort();
    ASSERT_GT(port, 0);
}

TEST(NetworkUtils, toHostAddr) {
    auto s = NetworkUtils::resolveHost("localhost", 1200);
    ASSERT_TRUE(s.ok());
    auto addr = s.value();
    IPv4 ip;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt("127.0.0.1", ip));
    ASSERT_EQ(addr[0].first, ip);
    ASSERT_EQ(addr[0].second, 1200);

    auto s2 = NetworkUtils::toHostAddr("8.8.8.8", 1300);
    ASSERT_TRUE(s2.ok());
    auto addr2 = s2.value();

    ASSERT_TRUE(NetworkUtils::ipv4ToInt("8.8.8.8", ip));
    ASSERT_EQ(addr2.first, ip);
    ASSERT_EQ(addr2.second, 1300);

    s2 = NetworkUtils::toHostAddr("a.b.c.d:a23", 1200);
    ASSERT_FALSE(s2.ok());
}

TEST(NetworkUtils, toHosts) {
    auto s = NetworkUtils::toHosts("localhost:1200, 127.0.0.1:1200");
    ASSERT_TRUE(s.ok());
    auto addr = s.value();

    IPv4 ip;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt("127.0.0.1", ip));
    ASSERT_EQ(addr[0].first, ip);
    ASSERT_EQ(addr[0].second, 1200);

    ASSERT_EQ(addr[1].first, ip);
    ASSERT_EQ(addr[1].second, 1200);

    s = NetworkUtils::toHosts("1.1.2.3:123, a.b.c.d:a23");
    ASSERT_FALSE(s.ok());
}

}   // namespace network
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
