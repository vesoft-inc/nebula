/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "network/NetworkUtils.h"

using namespace vesoft::network;


TEST(NetworkUtils, getHostname) {
    std::string hostname = NetworkUtils::getHostname();

    FILE* fp = popen("LD_PRELOAD= hostname -f | tr -d ['\n']", "r");
    char buffer[256];
    fgets(buffer, sizeof(buffer), fp);
    std::string hostnameCheck = buffer;

    EXPECT_EQ(std::string(buffer), hostname);
}


TEST(NetworkUtils, getLocalIPs) {
    std::vector<std::string> ipList = NetworkUtils::getLocalIPs(false);
    // We expect to have at least one real ip
    EXPECT_LE(1, ipList.size());

    for (auto& ip : ipList) {
        EXPECT_NE(ip, "127.0.0.1");
        LOG(INFO) << "ip: " << ip;
    }
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

