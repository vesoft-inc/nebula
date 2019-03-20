/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempFile.h"
#include "meta/MetaUtils.h"

namespace nebula {
namespace meta {

TEST(MetaUtilsTest, SpaceKeyTest) {
    auto prefix = MetaUtils::spacePrefix();
    ASSERT_EQ("__spaces__", prefix);
    auto spaceKey = MetaUtils::spaceKey(101);
    ASSERT_EQ(101, MetaUtils::spaceId(spaceKey));
    auto spaceVal = MetaUtils::spaceVal(100, 3, "default");
    ASSERT_EQ("default", MetaUtils::spaceName(spaceVal));
    ASSERT_EQ(100, *reinterpret_cast<const int32_t*>(spaceVal.c_str()));
    ASSERT_EQ(3, *reinterpret_cast<const int32_t*>(spaceVal.c_str() + sizeof(int32_t)));
}

TEST(MetaUtilsTest, PartKeyTest) {
    auto partKey = MetaUtils::partKey(0, 1);
    auto prefix = MetaUtils::partPrefix(0);
    ASSERT_EQ("__parts__", prefix.substr(0, prefix.size() - sizeof(GraphSpaceID)));
    ASSERT_EQ(0, *reinterpret_cast<const GraphSpaceID*>(
                        prefix.c_str() + prefix.size() - sizeof(GraphSpaceID)));
    ASSERT_EQ(prefix, partKey.substr(0, partKey.size() - sizeof(PartitionID)));
    ASSERT_EQ(1, *reinterpret_cast<const PartitionID*>(partKey.c_str() + prefix.size()));

    std::vector<cpp2::HostAddr> hosts;
    for (int i = 0; i < 10; i++) {
        cpp2::HostAddr host;
        host.set_ip(i * 20 + 1);
        host.set_port(i * 20 + 2);
        hosts.emplace_back(std::move(host));
    }
    auto partVal = MetaUtils::partVal(hosts);
    ASSERT_EQ(10 * sizeof(int32_t) * 2, partVal.size());
    auto result = MetaUtils::parsePartVal(partVal);
    ASSERT_EQ(hosts.size(), result.size());
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(i * 20 + 1, result[i].get_ip());
        ASSERT_EQ(i * 20 + 2, result[i].get_port());
    }
}

TEST(MetaUtilsTest, HostKeyTest) {
    auto hostKey = MetaUtils::hostKey(10, 11);
    const auto& prefix = MetaUtils::hostPrefix();
    ASSERT_EQ("__hosts__", prefix);
    ASSERT_EQ(prefix, hostKey.substr(0, hostKey.size() - 2 * sizeof(int32_t)));
    ASSERT_EQ(10, *reinterpret_cast<const IPv4*>(hostKey.c_str() + prefix.size()));
    ASSERT_EQ(11, *reinterpret_cast<const Port*>(hostKey.c_str() + prefix.size() + sizeof(IPv4)));

    auto addr = MetaUtils::parseHostKey(hostKey);
    ASSERT_EQ(10, addr.get_ip());
    ASSERT_EQ(11, addr.get_port());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


