/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "base/Base.h"
#include "fs/TempDir.h"
#include "test/ServerContext.h"
#include "graph/test/TestUtils.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "nebula/NebulaClient.h"
#include "nebula/ExecuteResponse.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {

TEST(CppClientTest, all) {
    FLAGS_load_data_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    nebula::fs::TempDir metaRootPath{"/tmp/MetaTest.XXXXXX"};
    nebula::fs::TempDir storageRootPath{"/tmp/StorageTest.XXXXXX"};
    // Create metaServer
    auto metaServer = nebula::meta::TestUtils::mockMetaServer(
                                            network::NetworkUtils::getAvailablePort(),
                                            metaRootPath.path(),
                                            kClusterId);
    auto meteServerPort = metaServer->port_;
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", meteServerPort);


    // Create storageServer
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto addrsRet
        = network::NetworkUtils::toHosts(folly::stringPrintf("127.0.0.1:%d", meteServerPort));
    CHECK(addrsRet.ok()) << addrsRet.status();
    auto storagePort = network::NetworkUtils::getAvailablePort();
    auto hostRet = nebula::network::NetworkUtils::toHostAddr("127.0.0.1", storagePort);
    if (!hostRet.ok()) {
        LOG(ERROR) << "Bad local host addr, status:" << hostRet.status();
    }
    auto& localhost = hostRet.value();

    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      std::move(addrsRet.value()),
                                                      localhost,
                                                      kClusterId,
                                                      true);
    mClient->waitForMetadReady();

    IPv4 localIp;
    nebula::network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto storageServer = nebula::storage::TestUtils::mockStorageServer(mClient.get(),
                                                  storageRootPath.path(),
                                                  localIp,
                                                  storagePort,
                                                  true);

    // Create graphServer
    auto graphServer = TestUtils::mockGraphServer(0);

    auto graphServerPort = graphServer->port_;
    sleep(FLAGS_load_data_interval_secs);

    NebulaClient client("127.0.0.1", graphServerPort);
    auto code = client.connect("user", "password");
    if (code != kSucceed) {
        LOG(INFO) << "Connect graphd failed, code: " << code;
        return;
    }
    // execute cmd: show spaces
    ExecuteResponse resp;
    std::string cmd = "SHOW SPACES";
    code = client.execute(cmd, resp);
    if (resp.getRows() != nullptr) {
        ASSERT_EQ(resp.getRows()->size(), 0);
    }

    cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "USE mySpace";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "CREATE TAG person(name string, age int, birthday timestamp, isBoy bool)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "CREATE EDGE schoolmate(likeness double)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    sleep(FLAGS_load_data_interval_secs + 3);

    cmd = "INSERT VERTEX person(name, age, birthday, isBoy) "
          "VALUES hash(\"Aero\"):(\"Aero\", 10, \"2009-10-01 10:00:00\", false)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "INSERT VERTEX person(name, age, birthday, isBoy) "
          "VALUES hash(\"Tom\"):(\"Tom\", 11, \"2008-07-01 14:00:00\", true)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "INSERT VERTEX person(name, age, birthday, isBoy) "
          "VALUES hash(\"Lily\"):(\"Lily\", 9, \"2010-08-01 10:00:00\", false)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "INSERT EDGE schoolmate(likeness) VALUES "
          "hash(\"Aero\")->hash(\"Tom\"):(90.0), "
          "hash(\"Aero\")->hash(\"Lily\"):(88.5)";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);

    cmd = "GO FROM hash(\"Aero\") OVER schoolmate "
          "YIELD schoolmate._dst, $$.person.name, $$.person.age, "
          "$$.person.birthday, schoolmate.likeness, $$.person.isBoy";
    code = client.execute(cmd, resp);
    ASSERT_EQ(code, kSucceed);
    ASSERT_NE(resp.getRows(), nullptr);
    ASSERT_EQ(resp.getRows()->size(), 2);

    std::vector<std::tuple<int64_t, std::string, int64_t, int64_t, double, bool>> expected = {
            {std::hash<std::string>()("Lily"), "Lily", 9, 1280628000, 88.5, false},
            {std::hash<std::string>()("Tom"), "Tom", 11, 1214892000, 90.0, true},
    };
    ASSERT_NE(resp.getRows(), nullptr);
    ASSERT_EQ(resp.getRows()->size(), expected.size());
    std::vector<std::tuple<int64_t, std::string, int64_t, int64_t, double, bool>> result;
    for (auto row : *resp.getRows()) {
        auto cols = row.getColumns();
        ASSERT_EQ(cols.size(), 6);
        ASSERT_EQ(cols[0].getType(), kIdType);
        ASSERT_EQ(cols[1].getType(), kStringType);
        ASSERT_EQ(cols[2].getType(), kIntType);
        ASSERT_EQ(cols[3].getType(), kTimestampType);
        ASSERT_EQ(cols[4].getType(), kDoubleType);
        ASSERT_EQ(cols[5].getType(), kBoolType);
        result.emplace_back(std::make_tuple(cols[0].getIdValue(),
                                            cols[1].getStrValue(),
                                            cols[2].getIntValue(),
                                            cols[3].getTimestampValue(),
                                            cols[4].getDoubleValue(),
                                            cols[5].getBoolValue()));
    }
    std::sort(result.begin(), result.end());
    std::sort(expected.begin(), expected.end());
    for (auto i = 0u; i < result.size(); i++) {
        if (result[i] != expected[i]) {
            ASSERT_TRUE(testing::AssertionFailure() << "result vs expect: "
                                                    << std::get<0>(result[i]) << ","
                                                    << std::get<1>(result[i]) << ", "
                                                    << std::get<2>(result[i]) << ", "
                                                    << std::get<3>(result[i]) << ", "
                                                    << std::get<4>(result[i]) << ", "
                                                    << std::get<5>(result[i]) << " vs "
                                                    << std::get<0>(expected[i]) << ", "
                                                    << std::get<1>(expected[i]) << ", "
                                                    << std::get<2>(expected[i]) << ", "
                                                    << std::get<3>(expected[i]) << ", "
                                                    << std::get<4>(expected[i]) << ", "
                                                    << std::get<5>(expected[i]));
        }
    }
}
}   // namespace graph
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
