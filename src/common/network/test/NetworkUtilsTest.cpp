/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/network/NetworkUtils.h"

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

TEST(NetworkUtils, getIPFromDevice) {
  {
    auto result = NetworkUtils::getIPFromDevice("lo");
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result.value() == "127.0.0.1" || result.value() == "::1");
  }
  {
    auto result = NetworkUtils::getIPFromDevice("any");
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ("0.0.0.0", result.value());
  }
  {
    auto result = NetworkUtils::getIPFromDevice("nonexistent");
    ASSERT_FALSE(result.ok()) << result.status();
  }
}

TEST(NetworkUtils, listIPs) {
  auto result = NetworkUtils::listIPs();
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_FALSE(result.value().empty());
  auto foundIPv4 = false;
  // auto foundIPv6 = false;
  for (auto& ip : result.value()) {
    if (ip == "127.0.0.1") {
      foundIPv4 = true;
    }
    // if (ip == "::1") {
    //   foundIPv6 = true;
    // }
  }
  ASSERT_TRUE(foundIPv4);
  // ASSERT_TRUE(foundIPv6); // This may fail on some OS without IPv6 support
}

TEST(NetworkUtils, listDeviceAndIPs) {
  auto result = NetworkUtils::listDeviceAndIPs();
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_FALSE(result.value().empty());
  ASSERT_NE(result.value().end(),
            std::find_if(result.value().begin(), result.value().end(), [](const auto& deviceAndIp) {
              return deviceAndIp.first == "lo";
            }));
  // Requires IPv6 env for testing
  // ASSERT_NE(result.value().end(),
  //           std::find_if(
  //  result.value().begin(), result.value().end(), [](const auto& deviceAndIp) {
  //             return deviceAndIp.second == "::1";
  //           }));
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

TEST(NetworkUtils, toHosts) {
  auto s = NetworkUtils::toHosts("localhost:1200, 127.0.0.1:1200");
  ASSERT_TRUE(s.ok());
  auto addr = s.value();

  ASSERT_EQ(addr[0].host, "localhost");
  ASSERT_EQ(addr[0].port, 1200);

  ASSERT_EQ(addr[1].host, "127.0.0.1");
  ASSERT_EQ(addr[1].port, 1200);

  s = NetworkUtils::toHosts("1.1.2.3:123, a.b.c.d:a23");
  ASSERT_FALSE(s.ok());

  s = NetworkUtils::toHosts("[::1]:1200, localhost:1200");
  ASSERT_TRUE(s.ok());
}

TEST(NetworkUtils, ValidateHostOrIp) {
  std::string hostOrIp = "127.0.0.1";
  auto result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "127.0.1.1";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "0.0.0.0";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "000.000.000.000";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_FALSE(result.ok());

  hostOrIp = "0.0.0.0.0";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_FALSE(result.ok());

  hostOrIp = "127.0.0.0.1";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_FALSE(result.ok());

  hostOrIp = "localhost";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "NonvalidHostName";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_FALSE(result.ok());

  hostOrIp = "lab.vesoft-inc.com";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "::1";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_TRUE(result.ok());

  hostOrIp = "::g";
  result = NetworkUtils::validateHostOrIp(hostOrIp);
  EXPECT_FALSE(result.ok());
}

}  // namespace network
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
