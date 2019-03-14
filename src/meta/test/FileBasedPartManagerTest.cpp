/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "meta/PartManager.h"
#include "meta/SchemaManager.h"
#include "fs/TempFile.h"
#include "network/NetworkUtils.h"

DECLARE_string(partition_conf_file);
DECLARE_string(schema_file);

namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;

void prepareConfFile() {
    static fs::TempFile confFile("/tmp/file_based_part_manager.XXXXXX");
    std::ofstream fs(confFile.path());
    fs << "{"
       << "  \"space_one\" : {"
       << "    \"1.1.1.1:1111\" : [1, 3, 5, 7, 9],"
       << "    \"2.2.2.2:2222\" : [2, 4, 6, 8, 10]"
       << "  },"
       << "  \"space_two\" : {"
       << "    \"3.3.3.3:3333\" : [1, 2, 3, 4, 5],"
       << "    \"4.4.4.4:4444\" : [6, 7, 8, 9, 10]"
       << "  }"
       << "}";
    fs.close();

    FLAGS_partition_conf_file = confFile.path();
    // To set a fake schema file so that FileBasedSchemaManager::toGraphSpaceID()
    // etc will be used
    FLAGS_schema_file = "fake_schema_file";
}


TEST(FileBasedPartManager, PartitionAllocation) {
    auto pm1 = PartManager::get(SchemaManager::toGraphSpaceID("space_one"));
    EXPECT_EQ(10, pm1->numParts());
    EXPECT_EQ(2, pm1->numHosts());
    auto hosts = pm1->hostsForPart(1);
    EXPECT_EQ(1, hosts.size());
    EXPECT_EQ("1.1.1.1", NetworkUtils::ipFromHostAddr(hosts[0]));
    EXPECT_EQ(5L, pm1->partsOnHost(NetworkUtils::toHostAddr("2.2.2.2:2222")).size());

    auto pm2 = PartManager::get(SchemaManager::toGraphSpaceID("space_two"));
    EXPECT_EQ(10, pm2->numParts());
    EXPECT_EQ(2, pm2->numHosts());
    hosts = pm2->hostsForPart(2);
    EXPECT_EQ(1, hosts.size());
    EXPECT_EQ("3.3.3.3", NetworkUtils::ipFromHostAddr(hosts[0]));
    EXPECT_EQ(5L, pm2->partsOnHost(NetworkUtils::toHostAddr("4.4.4.4:4444")).size());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    nebula::meta::prepareConfFile();

    return RUN_ALL_TESTS();
}


