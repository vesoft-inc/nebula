/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/IPAddressV4.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempFile.h"
#include "common/network/NetworkUtils.h"
#include "common/utils/MetaKeyUtils.h"

namespace nebula {

TEST(MetaKeyUtilsTest, SpaceKeyTest) {
  auto prefix = MetaKeyUtils::spacePrefix();
  ASSERT_EQ("__spaces__", prefix);
  auto spaceKey = MetaKeyUtils::spaceKey(101);
  ASSERT_EQ(101, MetaKeyUtils::spaceId(spaceKey));
  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default";
  spaceDesc.partition_num_ref() = 100;
  spaceDesc.replica_factor_ref() = 3;
  auto spaceVal = MetaKeyUtils::spaceVal(spaceDesc);
  ASSERT_EQ("default", MetaKeyUtils::spaceName(spaceVal));
  auto properties = MetaKeyUtils::parseSpace(spaceVal);
  ASSERT_EQ(100, properties.get_partition_num());
  ASSERT_EQ(3, properties.get_replica_factor());
}

TEST(MetaKeyUtilsTest, SpaceKeyWithZonesTest) {
  auto prefix = MetaKeyUtils::spacePrefix();
  ASSERT_EQ("__spaces__", prefix);
  auto spaceKey = MetaKeyUtils::spaceKey(101);
  ASSERT_EQ(101, MetaKeyUtils::spaceId(spaceKey));
  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default";
  spaceDesc.partition_num_ref() = 100;
  spaceDesc.replica_factor_ref() = 3;
  std::vector<std::string> zoneNames{"z1", "z2", "z3"};
  spaceDesc.zone_names_ref() = std::move(zoneNames);
  auto spaceVal = MetaKeyUtils::spaceVal(spaceDesc);
  ASSERT_EQ("default", MetaKeyUtils::spaceName(spaceVal));
  auto properties = MetaKeyUtils::parseSpace(spaceVal);
  ASSERT_EQ(100, properties.get_partition_num());
  ASSERT_EQ(3, properties.get_replica_factor());
  ASSERT_EQ(3, properties.get_zone_names().size());
}

TEST(MetaKeyUtilsTest, PartKeyTest) {
  auto partKey = MetaKeyUtils::partKey(0, 1);
  auto prefix = MetaKeyUtils::partPrefix(0);
  ASSERT_EQ("__parts__", prefix.substr(0, prefix.size() - sizeof(GraphSpaceID)));
  ASSERT_EQ(0,
            *reinterpret_cast<const GraphSpaceID*>(prefix.c_str() + prefix.size() -
                                                   sizeof(GraphSpaceID)));
  ASSERT_EQ(prefix, partKey.substr(0, partKey.size() - sizeof(PartitionID)));
  ASSERT_EQ(1, *reinterpret_cast<const PartitionID*>(partKey.c_str() + prefix.size()));

  std::vector<HostAddr> hosts;
  for (int i = 0; i < 10; i++) {
    hosts.emplace_back(std::to_string(i * 20 + 1), i * 20 + 2);
  }
  auto partVal = MetaKeyUtils::partVal(hosts);
  ASSERT_GE(partVal.size(), 10 * sizeof(int32_t) * 2);
  auto result = MetaKeyUtils::parsePartVal(partVal);
  ASSERT_EQ(hosts.size(), result.size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(std::to_string(i * 20 + 1), result[i].host);
    ASSERT_EQ(i * 20 + 2, result[i].port);
  }
}

TEST(MetaKeyUtilsTest, storeStrIpCodecTest) {
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
    auto encodedVal = MetaKeyUtils::partValV2(hosts);
    auto decodedVal = MetaKeyUtils::parsePartVal(encodedVal);
    ASSERT_EQ(hosts.size(), decodedVal.size());
    for (int i = 0; i < N; i++) {
      LOG(INFO) << folly::sformat("hosts[{}]={}:{}", i, hostnames[i], ports[i]);
      ASSERT_EQ(hostnames[i], decodedVal[i].host);
      ASSERT_EQ(ports[i], decodedVal[i].port);
    }
  }

  {
    // kMachinesTable : key
    auto key = MetaKeyUtils::machineKey(hostnames[0], ports[0]);
    auto host = MetaKeyUtils::parseMachineKey(key);
    ASSERT_EQ(host.host, hostnames[0]);
    ASSERT_EQ(host.port, ports[0]);
  }
  {
    // kHostsTable : key
    auto key = MetaKeyUtils::hostKey(hostnames[0], ports[0]);
    auto host = MetaKeyUtils::parseHostKeyV2(key);
    ASSERT_EQ(host.host, hostnames[0]);
    ASSERT_EQ(host.port, ports[0]);
  }

  {
    // leadersTableV3 : key
    GraphSpaceID spaceId = 1;
    PartitionID partId = 10;
    int64_t termId = 999999;
    auto key = MetaKeyUtils::leaderKey(spaceId, partId);
    auto parsedLeaderKey = MetaKeyUtils::parseLeaderKeyV3(key);
    EXPECT_EQ(parsedLeaderKey.first, spaceId);
    EXPECT_EQ(parsedLeaderKey.second, partId);

    HostAddr host;
    host.host = "Hello-Kitty";
    host.port = 9527;
    auto leaderVal = MetaKeyUtils::leaderValV3(host, termId);
    auto parsedVal = MetaKeyUtils::parseLeaderValV3(leaderVal);

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

TEST(MetaKeyUtilsTest, storeStrIpBackwardCompatibilityTest) {
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
    auto decodedVal = MetaKeyUtils::parseHostKey(encodedVal);
    ASSERT_EQ(decodedVal.host, hostnames[0]);
    ASSERT_EQ(decodedVal.port, ports[0]);
  }
}

TEST(MetaKeyUtilsTest, HostKeyTest) {
  auto hostKey = MetaKeyUtils::hostKey("10", 11);
  const auto& prefix = MetaKeyUtils::hostPrefix();
  ASSERT_EQ("__hosts__", prefix);

  auto addr = MetaKeyUtils::parseHostKey(hostKey);
  ASSERT_EQ("10", addr.host);
  ASSERT_EQ(11, addr.port);
}

TEST(MetaKeyUtilsTest, TagTest) {
  meta::cpp2::Schema schema;
  std::vector<meta::cpp2::ColumnDef> cols;
  for (auto i = 1; i <= 3; i++) {
    meta::cpp2::ColumnDef column;
    column.name_ref() = folly::stringPrintf("col_%d", i);
    column.type.type_ref() = nebula::cpp2::PropertyType::INT64;
    cols.emplace_back(std::move(column));
  }
  for (auto i = 4; i <= 6; i++) {
    meta::cpp2::ColumnDef column;
    column.name_ref() = folly::stringPrintf("col_%d", i);
    column.type.type_ref() = nebula::cpp2::PropertyType::FLOAT;
    cols.emplace_back(std::move(column));
  }
  for (auto i = 7; i < 10; i++) {
    meta::cpp2::ColumnDef column;
    column.name_ref() = folly::stringPrintf("col_%d", i);
    column.type.type_ref() = nebula::cpp2::PropertyType::STRING;
    cols.emplace_back(std::move(column));
  }
  schema.columns_ref() = std::move(cols);
  auto val = MetaKeyUtils::schemaVal("test_tag", schema);
  auto parsedSchema = MetaKeyUtils::parseSchema(val);
  ASSERT_EQ(parsedSchema, schema);
}

TEST(MetaKeyUtilsTest, ZoneTest) {
  auto zoneKey = MetaKeyUtils::zoneKey("test_zone");
  ASSERT_EQ("test_zone", MetaKeyUtils::parseZoneName(zoneKey));

  std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
  auto zoneValue = MetaKeyUtils::zoneVal(nodes);
  ASSERT_EQ(nodes, MetaKeyUtils::parseZoneHosts(zoneValue));
}

TEST(MetaKeyUtilsTest, DiskPathsTest) {
  HostAddr addr{"192.168.0.1", 1234};
  GraphSpaceID spaceId = 1;
  std::string path = "/data/storage/test_part1";

  auto diskPartsKey = MetaKeyUtils::diskPartsKey(addr, spaceId, path);
  ASSERT_EQ(addr, MetaKeyUtils::parseDiskPartsHost(diskPartsKey));
  ASSERT_EQ(spaceId, MetaKeyUtils::parseDiskPartsSpace(diskPartsKey));
  ASSERT_EQ(path, MetaKeyUtils::parseDiskPartsPath(diskPartsKey));
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
