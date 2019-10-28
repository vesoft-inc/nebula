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

    std::vector<nebula::cpp2::HostAddr> hosts;
    for (int i = 0; i < 10; i++) {
        nebula::cpp2::HostAddr host;
        host.set_ip(i * 20 + 1);
        host.set_port(i * 20 + 2);
        hosts.emplace_back(std::move(host));
    }
    auto partVal = MetaServiceUtils::partVal(hosts);
    ASSERT_EQ(10 * sizeof(int32_t) * 2, partVal.size());
    auto result = MetaServiceUtils::parsePartVal(partVal);
    ASSERT_EQ(hosts.size(), result.size());
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(i * 20 + 1, result[i].get_ip());
        ASSERT_EQ(i * 20 + 2, result[i].get_port());
    }
}

TEST(MetaServiceUtilsTest, HostKeyTest) {
    auto hostKey = MetaServiceUtils::hostKey(10, 11);
    const auto& prefix = MetaServiceUtils::hostPrefix();
    ASSERT_EQ("__hosts__", prefix);
    ASSERT_EQ(prefix, hostKey.substr(0, hostKey.size() - 2 * sizeof(int32_t)));
    ASSERT_EQ(10, *reinterpret_cast<const IPv4*>(hostKey.c_str() + prefix.size()));
    ASSERT_EQ(11, *reinterpret_cast<const Port*>(hostKey.c_str() + prefix.size() + sizeof(IPv4)));

    auto addr = MetaServiceUtils::parseHostKey(hostKey);
    ASSERT_EQ(10, addr.get_ip());
    ASSERT_EQ(11, addr.get_port());
}

TEST(MetaServiceUtilsTest, TagTest) {
    nebula::cpp2::Schema schema;
    decltype(schema.columns) cols;
    for (auto i = 1; i <= 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        nebula::cpp2::ValueType vType;
        vType.set_type(nebula::cpp2::SupportedType::INT);
        column.set_type(std::move(vType));
        cols.emplace_back(std::move(column));
    }
    for (auto i = 4; i <= 6; i++) {
        nebula::cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        nebula::cpp2::ValueType vType;
        vType.set_type(nebula::cpp2::SupportedType::FLOAT);
        column.set_type(std::move(vType));
        cols.emplace_back(std::move(column));
    }
    for (auto i = 7; i < 10; i++) {
        nebula::cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", i));
        nebula::cpp2::ValueType vType;
        vType.set_type(nebula::cpp2::SupportedType::STRING);
        column.set_type(std::move(vType));
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

