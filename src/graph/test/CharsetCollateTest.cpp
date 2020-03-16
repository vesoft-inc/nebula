/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class CharsetCollateTest : public TestBase {
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


TEST_F(CharsetCollateTest, ShowCharset) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW CHARSET";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string,
                    std::string, int>> expected {
            {"utf8", "UTF-8 Unicode", "utf8_bin", 4},
            {"utf8mb4", "UTF-8 Unicode", "utf8mb4_bin", 4},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW CHARSETS";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}


TEST_F(CharsetCollateTest, ShowCollate) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATION";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected {
            {"utf8_bin", "utf8"},
            {"utf8_general_ci", "utf8"},
            {"utf8mb4_bin", "utf8mb4"},
            {"utf8mb4_general_ci", "utf8mb4"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATE";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATIONS";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}


TEST_F(CharsetCollateTest, OrderByUtf8BinCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8, collate=utf8_bin)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           " VALUES 1:(\"Kobe\",\"america\"),"
                           "2:(\"Jordan\",\"Ameri\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON person 1,2,3,4,5,6,7,8,9 YIELD person.name as name,"
                           " person.country as country | ORDER BY country";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {8, "1234",       "1234"   },
            {9, "3456",       "3456"   },
            {2, "Jordan",     "Ameri"},
            {4, "Yaoming",    "Chin"  },
            {1, "Kobe",       "america"},
            {5, "Wangzhizhi", "china"  },
            {6, "Yijianlian", "中国"   },
            {7, "Duncan",     "法国"   },
            {3, "James",      "美国"   }
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON like 100->1,100->2,100->3,100->4,100->5,100->6,"
                           "100->7,100->8,100->9 YIELD like.city as city | ORDER BY city";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"like._src"},
            {"like._dst"},
            {"like._rank"},
            {"city"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, std::string>> expected = {
            {100, 8, 0, "1234"},
            {100, 9, 0, "3456"},
            {100, 1, 0, "Chicagok"},
            {100, 4, 0, "Shanghai"},
            {100, 5, 0, "beijing"},
            {100, 2, 0, "losAngeles"},
            {100, 7, 0, "巴黎"},
            {100, 6, 0, "广东"},
            {100, 3, 0, "洛杉矶"},
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.city AS city, "
                          "$$.person.country AS country | ORDER BY country";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"1234", "1234"},
            {"3456", "3456"},
            {"losAngeles", "Ameri"},
            {"Shanghai", "Chin"},
            {"Chicagok", "america"},
            {"beijing", "china"},
            {"广东", "中国"},
            {"巴黎", "法国"},
            {"洛杉矶", "美国"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(CharsetCollateTest, OrderByUtf8GenCiCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE spaceCI(partition_num=1, "
                           "replica_factor=1, charset=utf8, collate=utf8_general_ci)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE spaceCI";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           " VALUES 1:(\"Kobe\",\"america\"),"
                           "2:(\"Jordan\",\"Ameri\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON person 1,2,3,4,5,6,7,8,9 YIELD person.name as name,"
                           " person.country as country | ORDER BY country";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {8, "1234",       "1234"   },
            {9, "3456",       "3456"   },
            {2, "Jordan",     "Ameri"},
            {1, "Kobe",       "america"},
            {4, "Yaoming",    "Chin"  },
            {5, "Wangzhizhi", "china"  },
            {6, "Yijianlian", "中国"   },
            {7, "Duncan",     "法国"   },
            {3, "James",      "美国"   }
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON like 100->1,100->2,100->3,100->4,100->5,100->6,"
                           "100->7,100->8,100->9 YIELD like.city as city | ORDER BY city";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"like._src"},
            {"like._dst"},
            {"like._rank"},
            {"city"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, std::string>> expected = {
            {100, 8, 0, "1234"},
            {100, 9, 0, "3456"},
            {100, 5, 0, "beijing"},
            {100, 1, 0, "Chicagok"},
            {100, 2, 0, "losAngeles"},
            {100, 4, 0, "Shanghai"},
            {100, 7, 0, "巴黎"},
            {100, 6, 0, "广东"},
            {100, 3, 0, "洛杉矶"},
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.city AS city, "
                          "$$.person.country AS country | ORDER BY country";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"1234", "1234"},
            {"3456", "3456"},
            {"losAngeles", "Ameri"},
            {"Chicagok", "america"},
            {"Shanghai", "Chin"},
            {"beijing", "china"},
            {"广东", "中国"},
            {"巴黎", "法国"},
            {"洛杉矶", "美国"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE spaceCI";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(CharsetCollateTest, OrderByUtf8mb4BinCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8mb4, collate=utf8mb4_bin)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           " VALUES 1:(\"Kobe\",\"america\"),"
                           "2:(\"Jordan\",\"Ameri\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\"),"
                           "10:(\"😜\",\"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON person 1,2,3,4,5,6,7,8,9,10 YIELD person.name as name,"
                           " person.country as country | ORDER BY country";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {8, "1234",       "1234"   },
            {9, "3456",       "3456"   },
            {2, "Jordan",     "Ameri"},
            {4, "Yaoming",    "Chin"  },
            {1, "Kobe",       "america"},
            {5, "Wangzhizhi", "china"  },
            {6, "Yijianlian", "中国"   },
            {7, "Duncan",     "法国"   },
            {3, "James",      "美国"   },
            {10, "😜",        "😜"     }
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\"),"
                           "100->10:(\"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON like 100->1,100->2,100->3,100->4,100->5,100->6,"
                           "100->7,100->8,100->9,100->10 YIELD like.city as city | ORDER BY city";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"like._src"},
            {"like._dst"},
            {"like._rank"},
            {"city"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, std::string>> expected = {
            {100, 8, 0, "1234"},
            {100, 9, 0, "3456"},
            {100, 1, 0, "Chicagok"},
            {100, 4, 0, "Shanghai"},
            {100, 5, 0, "beijing"},
            {100, 2, 0, "losAngeles"},
            {100, 7, 0, "巴黎"},
            {100, 6, 0, "广东"},
            {100, 3, 0, "洛杉矶"},
            {100, 10, 0, "😜"}
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.city AS city, "
                          "$$.person.country AS country | ORDER BY country";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"1234", "1234"},
            {"3456", "3456"},
            {"losAngeles", "Ameri"},
            {"Shanghai", "Chin"},
            {"Chicagok", "america"},
            {"beijing", "china"},
            {"广东", "中国"},
            {"巴黎", "法国"},
            {"洛杉矶", "美国"},
            {"😜", "😜"}
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(CharsetCollateTest, OrderByUtf8mb4GenCiCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE spaceCI(partition_num=1, "
                           "replica_factor=1, charset=utf8mb4, collate=utf8mb4_general_ci)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE spaceCI";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           " VALUES 1:(\"Kobe\",\"america\"),"
                           "2:(\"Jordan\",\"Ameri\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\"),"
                           "10:(\"😜\", \"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON person 1,2,3,4,5,6,7,8,9,10 YIELD person.name as name,"
                           " person.country as country | ORDER BY country";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {8, "1234",       "1234"   },
            {9, "3456",       "3456"   },
            {2, "Jordan",     "Ameri"},
            {1, "Kobe",       "america"},
            {4, "Yaoming",    "Chin"  },
            {5, "Wangzhizhi", "china"  },
            {6, "Yijianlian", "中国"   },
            {7, "Duncan",     "法国"   },
            {3, "James",      "美国"   },
            {10, "😜",        "😜"     }
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\"),"
                           "100->10:(\"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "FETCH PROP ON like 100->1,100->2,100->3,100->4,100->5,100->6,"
                           "100->7,100->8,100->9, 100->10 YIELD like.city as city | ORDER BY city";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        std::vector<std::string> expectedColNames{
            {"like._src"},
            {"like._dst"},
            {"like._rank"},
            {"city"}
        };
        ASSERT_TRUE(verifyColNames(resp2, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, std::string>> expected = {
            {100, 8, 0, "1234"},
            {100, 9, 0, "3456"},
            {100, 5, 0, "beijing"},
            {100, 1, 0, "Chicagok"},
            {100, 2, 0, "losAngeles"},
            {100, 4, 0, "Shanghai"},
            {100, 7, 0, "巴黎"},
            {100, 6, 0, "广东"},
            {100, 3, 0, "洛杉矶"},
            {100, 10, 0, "😜"}
        };
        ASSERT_TRUE(verifyResult(resp2, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.city AS city, "
                          "$$.person.country AS country | ORDER BY country";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"1234", "1234"},
            {"3456", "3456"},
            {"losAngeles", "Ameri"},
            {"Chicagok", "america"},
            {"Shanghai", "Chin"},
            {"beijing", "china"},
            {"广东", "中国"},
            {"巴黎", "法国"},
            {"洛杉矶", "美国"},
            {"😜", "😜"}
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE spaceCI";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(CharsetCollateTest, RelationalExpressionCompareByUtf8BinCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8, collate=utf8_bin)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           "VALUES 1:(\"Jordan\",\"Ameri\"),"
                           "2:(\"Kobe\",\"america\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\")";

        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "GO FROM 100 OVER like WHERE like.city > \"losAngeles\" and"
                           " like.city < \"广东\" YIELD like.city AS city, $$.person.country"
                           " AS country | ORDER BY city";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp3, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"巴黎", "法国"},
        };
        ASSERT_TRUE(verifyResult(resp3, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(CharsetCollateTest, RelationalExpressionCompareByUtf8GenCiCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8, collate=utf8_general_ci)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           "VALUES 1:(\"Jordan\",\"Ameri\"),"
                           "2:(\"Kobe\",\"america\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\")";

        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "GO FROM 100 OVER like WHERE like.city > \"losAngeles\" and"
                           " like.city < \"广东\" YIELD like.city AS city, $$.person.country"
                           " AS country | ORDER BY city";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp3, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"Shanghai", "Chin"},
            {"巴黎", "法国"},
        };
        ASSERT_TRUE(verifyResult(resp3, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(CharsetCollateTest, RelationalExpressionCompareByUtf8mb4BinCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8mb4, collate=utf8mb4_bin)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           "VALUES 1:(\"Jordan\",\"Ameri\"),"
                           "2:(\"Kobe\",\"america\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin`\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\"),"
                           "10:(\"😜\", \"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\"),"
                           "100->10:(\"😜\")";

        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "GO FROM 100 OVER like WHERE like.city > \"losAngeles\" and"
                           " like.city < \"广东\" YIELD like.city AS city, $$.person.country"
                           " AS country | ORDER BY city";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp3, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"巴黎", "法国"},
        };
        ASSERT_TRUE(verifyResult(resp3, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(CharsetCollateTest, RelationalExpressionCompareByUtf8mb4GenCiCollation) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=1, "
                           "replica_factor=1, charset=utf8mb4, collate=utf8mb4_general_ci)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(name string, country string)";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE Edge like(city string)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT VERTEX person(name, country)"
                           "VALUES 1:(\"Jordan\",\"Ameri\"),"
                           "2:(\"Kobe\",\"america\"),"
                           "3:(\"James\",\"美国\"),"
                           "4:(\"Yaoming\",\"Chin\"),"
                           "5:(\"Wangzhizhi\",\"china\"),"
                           "6:(\"Yijianlian\",\"中国\"),"
                           "7:(\"Duncan\",\"法国\"),"
                           "8:(\"1234\",\"1234\"),"
                           "9:(\"3456\",\"3456\"),"
                           "10:(\"😜\", \"😜\")";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "INSERT EDGE like(city)"
                           "VALUES 100->1:(\"Chicagok\"),"
                           "100->2:(\"losAngeles\"),"
                           "100->3:(\"洛杉矶\"),"
                           "100->4:(\"Shanghai\"),"
                           "100->5:(\"beijing\"),"
                           "100->6:(\"广东\"),"
                           "100->7:(\"巴黎\"),"
                           "100->8:(\"1234\"),"
                           "100->9:(\"3456\"),"
                           "100->10:(\"😜\")";

        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "GO FROM 100 OVER like WHERE like.city > \"losAngeles\" and"
                           " like.city < \"广东\" YIELD like.city AS city, $$.person.country"
                           " AS country | ORDER BY city";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        std::vector<std::string> expectedColNames{
            {"city"},
            {"country"}
        };
        ASSERT_TRUE(verifyColNames(resp3, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected = {
            {"Shanghai", "Chin"},
            {"巴黎", "法国"},
        };
        ASSERT_TRUE(verifyResult(resp3, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
