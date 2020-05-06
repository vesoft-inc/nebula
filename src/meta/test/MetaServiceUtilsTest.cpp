/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempFile.h"
#include "meta/MetaServiceUtils.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace meta {

TEST(MetaServiceUtilsTest, SpaceKeyTest) {
    auto prefix = MetaServiceUtils::spacePrefix();
    ASSERT_EQ("__spaces__", prefix);
    auto spaceKey = MetaServiceUtils::spaceKey(101);
    ASSERT_EQ(101, MetaServiceUtils::spaceId(spaceKey));
    cpp2::SpaceProperties properties;
    properties.set_space_name("default");
    properties.set_partition_num(100);
    properties.set_replica_factor(3);
    auto spaceVal = MetaServiceUtils::spaceVal(properties);
    ASSERT_EQ("default", MetaServiceUtils::spaceName(spaceVal));
    ASSERT_EQ(100, MetaServiceUtils::parseSpace(spaceVal).get_partition_num());
    ASSERT_EQ(3, MetaServiceUtils::parseSpace(spaceVal).get_replica_factor());
}

TEST(MetaServiceUtilsTest, PartKeyTest) {
    auto partKey = MetaServiceUtils::partKey(0, 1);
    auto prefix = MetaServiceUtils::partPrefix(0);
    ASSERT_EQ("__parts__", prefix.substr(0, prefix.size() - sizeof(GraphSpaceID)));
    ASSERT_EQ(0, *reinterpret_cast<const GraphSpaceID*>(
                        prefix.c_str() + prefix.size() - sizeof(GraphSpaceID)));
    ASSERT_EQ(prefix, partKey.substr(0, partKey.size() - sizeof(PartitionID)));
    ASSERT_EQ(1, *reinterpret_cast<const PartitionID*>(partKey.c_str() + prefix.size()));

    std::vector<HostAddr> hosts;
    for (int i = 0; i < 10; i++) {
        hosts.emplace_back(i * 20 + 1, i * 20 + 2);
    }
    {
        auto partVal = MetaServiceUtils::partValV1(hosts);
        ASSERT_EQ(10 * sizeof(int32_t) * 2, partVal.size());
        auto result = MetaServiceUtils::parsePartValV1(partVal);
        ASSERT_EQ(hosts.size(), result.size());
        for (int i = 0; i < 10; i++) {
            ASSERT_EQ(i * 20 + 1, result[i].ip);
            ASSERT_EQ(i * 20 + 2, result[i].port);
        }
    }
    {
        auto partVal = MetaServiceUtils::partValV2(hosts);
        auto result = MetaServiceUtils::parsePartValV2(partVal);
        ASSERT_EQ(hosts.size(), result.size());
        for (int i = 0; i < 10; i++) {
            ASSERT_EQ(i * 20 + 1, result[i].ip);
            ASSERT_EQ(i * 20 + 2, result[i].port);
        }
    }
}

TEST(MetaServiceUtilsTest, storeStrIpCodecTest) {
    int N = 4;
    std::vector<std::string> strIps(N);
    std::vector<int> ips(N);
    std::vector<int> ports;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < N; ++i) {
        strIps[i] = folly::sformat("192.168.8.{0}", i);
        ASSERT_TRUE(nebula::network::NetworkUtils::ipv4ToInt(strIps[i], ips[i]));
        ports.emplace_back(2000 + i);
        hosts.emplace_back(ips[i], ports[i]);
    }

    {
        // kPartsTable : value
        auto partVal = MetaServiceUtils::partValV2(hosts);
        auto result = MetaServiceUtils::parsePartValV2(partVal);
        ASSERT_EQ(hosts.size(), result.size());
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(ips[i], result[i].ip);
            LOG(INFO) << folly::format("ips[{0}]={1}", i,
                                        nebula::network::NetworkUtils::intToIPv4(ips[i]));
            ASSERT_EQ(ports[i], result[i].port);
        }
    }

    {
        // kHostsTable : key
        auto key = MetaServiceUtils::hostKey(ips[0], ports[0]);
        auto host = MetaServiceUtils::parseHostKeyV2(key);
        ASSERT_EQ(host.ip, ips[0]);
        ASSERT_EQ(host.port, ports[0]);
    }

    {
        // kLeadersTable : key
        auto key = MetaServiceUtils::leaderKey(ips[0], ports[0]);
        auto host = MetaServiceUtils::parseLeaderKeyV2(key);
        ASSERT_EQ(host.ip, ips[0]);
        ASSERT_EQ(host.port, ports[0]);
    }
}

std::string hostKeyV1(IPv4 ip, Port port) {
    const std::string kHostsTable          = "__hosts__";          // NOLINT
    std::string key;
    key.reserve(kHostsTable.size() + sizeof(IPv4) + sizeof(Port));
    key.append(kHostsTable.data(), kHostsTable.size())
       .append(reinterpret_cast<const char*>(&ip), sizeof(ip))
       .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string leaderKeyV1(IPv4 ip, Port port) {
    const std::string kLeadersTable        = "__leaders__";          // NOLINT
    std::string key;
    key.reserve(kLeadersTable.size() + sizeof(IPv4) + sizeof(Port));
    key.append(kLeadersTable.data(), kLeadersTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

TEST(MetaServiceUtilsTest, storeStrIpBackwardCompatibilityTest) {
    int N = 4;
    std::vector<std::string> strIps(N);
    std::vector<int> ips(N);
    std::vector<int> ports;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < N; ++i) {
        strIps[i] = folly::sformat("192.168.8.{0}", i);
        ASSERT_TRUE(nebula::network::NetworkUtils::ipv4ToInt(strIps[i], ips[i]));
        ports.emplace_back(2000 + i);
        hosts.emplace_back(ips[i], ports[i]);
    }

    {
        // kHostsTable : key
        auto key = hostKeyV1(ips[0], ports[0]);
        LOG(INFO) << "before parseHostKeyV2(key)";
        auto host = MetaServiceUtils::parseHostKey(key);
        LOG(INFO) << "after parseHostKeyV2(key)";
        ASSERT_EQ(host.ip, ips[0]);
        ASSERT_EQ(host.port, ports[0]);
    }

    {
        // kLeadersTable : key
        auto key = leaderKeyV1(ips[0], ports[0]);
        auto host = MetaServiceUtils::parseLeaderKey(key);
        ASSERT_EQ(host.ip, ips[0]);
        ASSERT_EQ(host.port, ports[0]);
    }
}

TEST(MetaServiceUtilsTest, HostKeyTest) {
    auto hostKey = MetaServiceUtils::hostKey(10, 11);
    const auto& prefix = MetaServiceUtils::hostPrefix();
    const auto& ipPortStr = MetaServiceUtils::encodeHostAddrV2(10, 11);
    ASSERT_EQ("__hosts__", prefix);
    ASSERT_EQ(prefix, hostKey.substr(0, hostKey.size() - ipPortStr.size()));

    auto addr = MetaServiceUtils::parseHostKey(hostKey);
    ASSERT_EQ(10, addr.ip);
    ASSERT_EQ(11, addr.port);
}

TEST(MetaServiceUtilsTest, TagTest) {
    cpp2::Schema schema;
    decltype(schema.columns) cols;
    for (auto i = 1; i <= 3; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.set_type(cpp2::PropertyType::INT64);
        cols.emplace_back(std::move(column));
    }
    for (auto i = 4; i <= 6; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.set_type(cpp2::PropertyType::FLOAT);
        cols.emplace_back(std::move(column));
    }
    for (auto i = 7; i < 10; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.set_type(cpp2::PropertyType::STRING);
        cols.emplace_back(std::move(column));
    }
    schema.set_columns(std::move(cols));
    auto val = MetaServiceUtils::schemaTagVal("test_tag", schema);
    auto parsedSchema = MetaServiceUtils::parseSchema(val);
    ASSERT_EQ(parsedSchema, schema);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

