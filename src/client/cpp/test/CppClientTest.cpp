/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "nebula/NebulaClient.h"
#include "nebula/ExecutionResponse.h"
#include "client/cpp/lib/ConnectionThread.h"
#include "client/cpp/lib/ConnectionPool.h"
#include "thread/NamedThread.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class CppClientTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};

TEST_F(CppClientTest, all) {
    auto serverPort = gEnv->graphServerPort();
    auto cb = [serverPort] (const std::string &name) {
        NebulaClient client("127.0.0.1", serverPort);
        auto code = client.authenticate("user", "password");
        if (code != kSucceed) {
            LOG(ERROR) << "Authenticate graphd failed, code: " << code;
            return;
        }
        // execute cmd: show spaces
        ExecutionResponse resp;
        auto *format = "CREATE SPACE mySpace%s(partition_num=1, replica_factor=1)";
        auto cmd = folly::stringPrintf(format, name.c_str());
        code = client.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);

        cmd = folly::stringPrintf("USE mySpace%s", name.c_str());
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

        auto checkFun = [] (ExecutionResponse* response, ErrorCode resultCode) {
            LOG(INFO) << "Now into checkFun...";
            ASSERT_EQ(resultCode, kSucceed);
            using resultType = std::tuple<int64_t, std::string, int64_t, int64_t, double, bool>;
            std::vector<resultType> expected = {
                    {std::hash<std::string>()("Lily"), "Lily", 9, 1280628000, 88.5, false},
                    {std::hash<std::string>()("Tom"), "Tom", 11, 1214892000, 90.0, true},
            };
            ASSERT_EQ(response->getRows().size(), expected.size());
            std::vector<std::tuple<int64_t, std::string, int64_t, int64_t, double, bool>> result;
            for (auto row : response->getRows()) {
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
        };

        // async
        client.asyncExecute(cmd, checkFun);

        sleep(1);

        // sync
        code = client.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);
        ASSERT_EQ(resp.getRows().size(), 2);
        checkFun(&resp, code);

        cmd = "INSERT EDGE schoolmate(likeness) VALUES "
            "hash(\"Tom\")->hash(\"Lily\"):(90.0)";
        code = client.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);

        auto checkPath = [] (ExecutionResponse* response, ErrorCode resultCode) {
            LOG(INFO) << "To do checkPath.....";
            const std::string path1 = folly::stringPrintf("%ld<schoolmate,0>%ld",
                                    std::hash<std::string>()("Aero"),
                                    std::hash<std::string>()("Lily"));
            const std::string path2 = folly::stringPrintf("%ld<schoolmate,0>%ld<schoolmate,0>%ld",
                                    std::hash<std::string>()("Aero"),
                                    std::hash<std::string>()("Tom"),
                                    std::hash<std::string>()("Lily"));
            std::vector<std::string> expectPaths = {path1, path2};
            ASSERT_EQ(resultCode, kSucceed);
            ASSERT_EQ(response->getRows().size(), expectPaths.size());
            std::vector<std::string> paths;
            auto rows = response->getRows();
            for (auto &row : rows) {
                auto cols = row.getColumns();
                auto &pathValue = row.getColumns().back().getPathValue();
                auto entrys = pathValue.getEntryList();
                std::string pathStr;
                auto iter = entrys.begin();
                while (iter < (entrys.end() - 1)) {
                    pathStr += folly::stringPrintf("%ld<%s,%ld>",
                                    iter->getVertexValue().id,
                                    (iter + 1)->getEdgeValue().type.c_str(),
                                    (iter + 1)->getEdgeValue().ranking);
                    iter = iter + 2;
                }
                pathStr += folly::to<std::string>(iter->getVertexValue().id);
                paths.emplace_back(std::move(pathStr));
            }

            std::sort(paths.begin(), paths.end());
            std::sort(expectPaths.begin(), expectPaths.end());
            for (decltype(paths.size()) i = 0; i < paths.size(); ++i) {
                if (paths[i] != expectPaths[i]) {
                    ASSERT_TRUE(testing::AssertionFailure() << paths[i]
                            <<" vs. " << expectPaths[i]);
                }
            }
        };
        cmd = "FIND ALL PATH FROM hash(\"Aero\") TO hash(\"Lily\") OVER schoolmate UPTO 2 STEPS";
        client.asyncExecute(cmd, checkPath);
        code = client.execute(cmd, resp);
        checkPath(&resp, code);
    };

    thread::NamedThread thread1("thread1", cb, "1");
    thread::NamedThread thread2("thread2", cb, "2");
    thread1.join();
    thread2.join();
}

TEST_F(CppClientTest, testReuseConnection) {
    auto serverPort = gEnv->graphServerPort();
    {
        NebulaClient client1("127.0.0.1", serverPort);
        auto code = client1.authenticate("user", "password");
        if (code != kSucceed) {
            LOG(ERROR) << "Authenticate graphd failed, code: " << code;
            return;
        }
        ExecutionResponse resp;
        auto cmd = folly::stringPrintf("USE mySpace1");
        code = client1.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);
        cmd = folly::stringPrintf("CREATE TAG test()");
        code = client1.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);

        NebulaClient client2("127.0.0.1", serverPort);
        code = client2.authenticate("user", "password");
        if (code != kSucceed) {
            LOG(INFO) << "Authenticate graphd failed, code: " << code;
            return;
        }
        cmd = folly::stringPrintf("USE mySpace1");
        code = client2.execute(cmd, resp);
        ASSERT_EQ(code, kSucceed);
    }
    {
        NebulaClient client("127.0.0.1", serverPort);
        auto code = client.authenticate("user", "password");
        if (code != kSucceed) {
            LOG(ERROR) << "Authenticate graphd failed, code: " << code;
            return;
        }
        ExecutionResponse resp;
        auto cmd = folly::stringPrintf("CREATE TAG test()");
        code = client.execute(cmd, resp);
        ASSERT_NE(code, kSucceed);
    }
}
}   // namespace graph
}   // namespace nebula

