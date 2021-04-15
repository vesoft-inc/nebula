/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/network/NetworkUtils.h"
#include "common/fs/TempFile.h"
#include <gtest/gtest.h>
#include <folly/IPAddressV4.h>
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

TEST(MetaServiceUtilsTest, SpaceKeyTest) {
    auto prefix = MetaServiceUtils::spacePrefix();
    ASSERT_EQ("__spaces__", prefix);
    auto spaceKey = MetaServiceUtils::spaceKey(101);
    ASSERT_EQ(101, MetaServiceUtils::spaceId(spaceKey));
    cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default");
    spaceDesc.set_partition_num(100);
    spaceDesc.set_replica_factor(3);
    auto spaceVal = MetaServiceUtils::spaceVal(spaceDesc);
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
        hosts.emplace_back(std::to_string(i * 20 + 1), i * 20 + 2);
    }
    auto partVal = MetaServiceUtils::partVal(hosts);
    ASSERT_GE(partVal.size(), 10 * sizeof(int32_t) * 2);
    auto result = MetaServiceUtils::parsePartVal(partVal);
    ASSERT_EQ(hosts.size(), result.size());
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(std::to_string(i * 20 + 1), result[i].host);
        ASSERT_EQ(i * 20 + 2, result[i].port);
    }
}

TEST(MetaServiceUtilsTest, storeStrIpCodecTest) {
    int N = 4;
    std::vector<std::string> hostnames(N);
    std::vector<int> ports;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < N; ++i) {
        hostnames[i] = folly::sformat("192.168.8.{0}", i);
        ports.emplace_back(2000 + i);
        hosts.emplace_back(hostnames[i], ports[i]);
    }

    {
        // kPartsTable : value
        auto encodedVal = MetaServiceUtils::partValV2(hosts);
        auto decodedVal = MetaServiceUtils::parsePartVal(encodedVal);
        ASSERT_EQ(hosts.size(), decodedVal.size());
        for (int i = 0; i < N; i++) {
            LOG(INFO) << folly::format("hosts[{}]={}:{}", i, hostnames[i], ports[i]);
            ASSERT_EQ(hostnames[i], decodedVal[i].host);
            ASSERT_EQ(ports[i], decodedVal[i].port);
        }
    }

    {
        // kHostsTable : key
        auto key = MetaServiceUtils::hostKey(hostnames[0], ports[0]);
        auto host = MetaServiceUtils::parseHostKeyV2(key);
        ASSERT_EQ(host.host, hostnames[0]);
        ASSERT_EQ(host.port, ports[0]);
    }

    {
        // leadersTableV3 : key
        GraphSpaceID spaceId = 1;
        PartitionID partId = 10;
        int64_t termId = 999999;
        auto key = MetaServiceUtils::leaderKey(spaceId, partId);
        auto parsedLeaderKey = MetaServiceUtils::parseLeaderKeyV3(key);
        EXPECT_EQ(parsedLeaderKey.first, spaceId);
        EXPECT_EQ(parsedLeaderKey.second, partId);


        HostAddr host;
        host.host = "Hello-Kitty";
        host.port = 9527;
        auto leaderVal = MetaServiceUtils::leaderValV3(host, termId);
        auto parsedVal = MetaServiceUtils::parseLeaderValV3(leaderVal);

        EXPECT_EQ(std::get<0>(parsedVal), host);
        EXPECT_EQ(std::get<1>(parsedVal), termId);
    }
}

std::string hostKeyV1(uint32_t ip, Port port) {
    const std::string kHostsTable = "__hosts__";
    std::string key;
    key.reserve(kHostsTable.size() + sizeof(ip) + sizeof(Port));
    key.append(kHostsTable.data(), kHostsTable.size())
       .append(reinterpret_cast<const char*>(&ip), sizeof(ip))
       .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string leaderKeyV1(uint32_t ip, Port port) {
    const std::string kLeadersTable = "__leaders__";
    std::string key;
    key.reserve(kLeadersTable.size() + sizeof(ip) + sizeof(Port));
    key.append(kLeadersTable.data(), kLeadersTable.size())
       .append(reinterpret_cast<const char*>(&ip), sizeof(ip))
       .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

TEST(MetaServiceUtilsTest, storeStrIpBackwardCompatibilityTest) {
    int N = 4;
    std::vector<std::string> hostnames(N);
    std::vector<uint32_t> ips(N);
    std::vector<int> ports(N);
    std::vector<HostAddr> hosts(N);
    for (int i = 0; i < N; ++i) {
        hostnames[i] = folly::sformat("192.168.8.{0}", i);
        ips[i] = folly::IPAddressV4::toLongHBO(hostnames[i]);
        ports.emplace_back(2000 + i);
        hosts.emplace_back(hostnames[i], ports[i]);
    }

    {
        // kHostsTable : key
        auto encodedVal = hostKeyV1(ips[0], ports[0]);
        auto decodedVal = MetaServiceUtils::parseHostKey(encodedVal);
        ASSERT_EQ(decodedVal.host, hostnames[0]);
        ASSERT_EQ(decodedVal.port, ports[0]);
    }
}

TEST(MetaServiceUtilsTest, HostKeyTest) {
    auto hostKey = MetaServiceUtils::hostKey("10", 11);
    const auto& prefix = MetaServiceUtils::hostPrefix();
    ASSERT_EQ("__hosts__", prefix);

    auto addr = MetaServiceUtils::parseHostKey(hostKey);
    ASSERT_EQ("10", addr.host);
    ASSERT_EQ(11, addr.port);
}

TEST(MetaServiceUtilsTest, TagTest) {
    cpp2::Schema schema;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (auto i = 1; i <= 3; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.type.set_type(cpp2::PropertyType::INT64);
        cols.emplace_back(std::move(column));
    }
    for (auto i = 4; i <= 6; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.type.set_type(cpp2::PropertyType::FLOAT);
        cols.emplace_back(std::move(column));
    }
    for (auto i = 7; i < 10; i++) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        column.type.set_type(cpp2::PropertyType::STRING);
        cols.emplace_back(std::move(column));
    }
    schema.set_columns(std::move(cols));
    auto val = MetaServiceUtils::schemaVal("test_tag", schema);
    auto parsedSchema = MetaServiceUtils::parseSchema(val);
    ASSERT_EQ(parsedSchema, schema);
}

TEST(MetaServiceUtilsTest, GroupTest) {
    auto groupKey = MetaServiceUtils::groupKey("test_group");
    ASSERT_EQ("test_group", MetaServiceUtils::parseGroupName(groupKey));

    std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
    auto groupValue = MetaServiceUtils::groupVal(zones);
    ASSERT_EQ(zones, MetaServiceUtils::parseZoneNames(groupValue));
}

TEST(MetaServiceUtilsTest, ZoneTest) {
    auto zoneKey = MetaServiceUtils::zoneKey("test_zone");
    ASSERT_EQ("test_zone", MetaServiceUtils::parseZoneName(zoneKey));

    std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
    auto zoneValue = MetaServiceUtils::zoneVal(nodes);
    ASSERT_EQ(nodes, MetaServiceUtils::parseZoneHosts(zoneValue));
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
