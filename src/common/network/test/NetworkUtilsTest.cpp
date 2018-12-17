/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "network/NetworkUtils.h"

using namespace nebula::network;


TEST(NetworkUtils, getHostname) {
    std::string hostname = NetworkUtils::getHostname();

    FILE* fp = popen("LD_PRELOAD= hostname | tr -d ['\n']", "r");
    char buffer[256];
    fgets(buffer, sizeof(buffer), fp);
    std::string hostnameCheck = buffer;

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
    uint32_t ip;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt("127.0.0.1", ip));
    EXPECT_EQ(NetworkUtils::intToIPv4(ip), "127.0.0.1");

    ip = 0x11223344;
    uint32_t converted;
    ASSERT_TRUE(NetworkUtils::ipv4ToInt(NetworkUtils::intToIPv4(ip), converted));
    EXPECT_EQ(converted, ip);
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

