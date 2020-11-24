/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/encryption/MD5Utils.h"
#include "common/network/NetworkUtils.h"
#include "common/plugin/fulltext/FTUtils.h"
#include "common/plugin/fulltext/elasticsearch/ESStorageAdapter.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"

namespace nebula {
namespace plugin {

TEST(FulltextPluginTest, ESIndexCheckTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    auto ret = ESGraphAdapter().indexExistsCmd(client, "test_index");
    auto expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                    "-XGET \"http://127.0.0.1:9200/_cat/indices/test_index?format=json\"";
    ASSERT_EQ(expected, ret);
}

TEST(FulltextPluginTest, ESCreateIndexTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    auto ret = ESGraphAdapter().createIndexCmd(client, "test_index");
    auto expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                    "-XPUT \"http://127.0.0.1:9200/test_index\"";
    ASSERT_EQ(expected, ret);
}

TEST(FulltextPluginTest, ESDropIndexTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    auto ret = ESGraphAdapter().dropIndexCmd(client, "test_index");
    auto expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                    "-XDELETE \"http://127.0.0.1:9200/test_index\"";
    ASSERT_EQ(expected, ret);
}

TEST(FulltextPluginTest, ESPutTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient hc(localHost_);
    DocItem item("index1", "col1", 1, 2, "aaaa");
    auto ret = ESStorageAdapter().putCmd(hc, item);
    auto expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                    "-XPUT \"http://127.0.0.1:9200/index1/_doc/"
                    "000000000100000000028c43de7b01bca674276c43e09b3ec5baYWFhYQ==\" "
                    "-d'{\"value\":\"aaaa\",\"schema_id\":2,\"column_id\""
                    ":\"8c43de7b01bca674276c43e09b3ec5ba\"}'";
    ASSERT_EQ(expected, ret);
}

TEST(FulltextPluginTest, ESBulkTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient hc(localHost_);
    DocItem item1("index1", "col1", 1, 2, "aaaa");
    DocItem item2("index1", "col1", 1, 2, "bbbb");
    std::set<std::string> strs;
    strs.emplace("aaa");
    strs.emplace("bbb");
    std::vector<DocItem> items;
    items.emplace_back(DocItem("index1", "col1", 1, 2, "aaaa"));
    items.emplace_back(DocItem("index1", "col1", 1, 2, "bbbb"));
    auto ret = ESStorageAdapter().bulkCmd(hc, items);
    auto expected = "/usr/bin/curl -H \"Content-Type: application/x-ndjson; "
                    "charset=utf-8\" -XPOST \"http://127.0.0.1:9200/_bulk\" "
                    "-d '\n{\"index\":{\"_index\":\"index1\",\"_id\":"
                    "\"000000000100000000028c43de7b01bca674276c43e09b3ec5baYWFhYQ==\"}}"
                    "\n{\"schema_id\":2,\"value\":\"aaaa\",\"column_id\":"
                    "\"8c43de7b01bca674276c43e09b3ec5ba\"}\n{\"index\":"
                    "{\"_index\":\"index1\",\"_id\":"
                    "\"000000000100000000028c43de7b01bca674276c43e09b3ec5baYmJiYg=="
                    "\"}}\n{\"schema_id\":2,\"value\":\"bbbb\",\"column_id\":"
                    "\"8c43de7b01bca674276c43e09b3ec5ba\"}\n'";
    ASSERT_EQ(expected, ret);
}

TEST(FulltextPluginTest, ESPutToTest) {
    HostAddr localHost_{"127.0.0.1", 11111};
    HttpClient hc(localHost_);
    DocItem item1("index1", "col1", 1, 2, "aaaa");
    // A ElasticSearch instance needs to be turn on at here, so expected false.
    auto ret = ESStorageAdapter::kAdapter->put(hc, item1);
    ASSERT_FALSE(ret);
}

TEST(FulltextPluginTest, ESResultTest) {
    //    {
    //        "took": 2,
    //            "timed_out": false,
    //            "_shards": {
    //            "total": 1,
    //                "successful": 1,
    //                "skipped": 0,
    //                "failed": 0
    //        },
    //        "hits": {
    //            "total": {
    //                "value": 1,
    //                    "relation": "eq"
    //            },
    //            "max_score": 3.3862944,
    //                "hits": [
    //                {
    //                    "_index": "my_temp_index_3",
    //                        "_type": "_doc",
    //                        "_id": "part1_tag1_col1_aaa",
    //                        "_score": 3.3862944,
    //                        "_source": {
    //                        "value": "aaa"
    //                    }
    //                },
    //                {
    //                    "_index": "my_temp_index_3",
    //                        "_type": "_doc",
    //                        "_id": "part2_tag2_col1_bbb",
    //                        "_score": 1.0,
    //                        "_source": {
    //                        "value": "bbb"
    //                    }
    //                }
    //            ]
    //        }
    //    }
    {
        std::string json = R"({"took": 2,"timed_out": false,"_shards": {"total": 1,"successful": 1,
                          "skipped": 0,"failed": 0},"hits": {"total": {"value": 1,"relation":
                          "eq"},"max_score": 3.3862944,"hits": [{"_index": "my_temp_index_3",
                          "_type": "_doc","_id": "part1_tag1_col1_aaa","_score": 3.3862944,
                          "_source": {"value": "aaa"}},{"_index": "my_temp_index_3","_type":
                          "_doc","_id": "part2_tag2_col1_bbb","_score": 1.0,
                          "_source": {"value": "bbb"}}]}})";
        HostAddr localHost_{"127.0.0.1", 9200};
        HttpClient hc(localHost_);
        std::vector<std::string> rows;
        auto ret = ESGraphAdapter().result(json, rows);
        ASSERT_TRUE(ret);
        std::vector<std::string> expect = {"aaa", "bbb"};
        ASSERT_EQ(expect, rows);
    }

    //    {
    //        "took": 1,
    //            "timed_out": false,
    //            "_shards": {
    //            "total": 1,
    //                "successful": 1,
    //                "skipped": 0,
    //                "failed": 0
    //        },
    //        "hits": {
    //            "total": {
    //                "value": 0,
    //                    "relation": "eq"
    //            },
    //            "max_score": null,
    //                "hits": []
    //        }
    //    }
    {
        std::string json = R"({"took": 1,"timed_out": false,"_shards": {"total": 1,"successful": 1,
                             "skipped": 0,"failed": 0},"hits": {"total":
                             {"value": 0,"relation": "eq"},"max_score": null,"hits": []}})";

        HostAddr localHost_{"127.0.0.1", 9200};
        HttpClient hc(localHost_);
        std::vector<std::string> rows;
        auto ret = ESGraphAdapter().result(json, rows);
        ASSERT_TRUE(ret);
        ASSERT_EQ(0, rows.size());
    }

    //    {
    //        "error": {
    //            "root_cause": [
    //            {
    //                "type": "parsing_exception",
    //                    "reason": "Unknown key for a VALUE_STRING in [_soure].",
    //                    "line": 1,
    //                    "col": 128
    //            }
    //            ],
    //            "type": "parsing_exception",
    //                "reason": "Unknown key for a VALUE_STRING in [_soure].",
    //                "line": 1,
    //                "col": 128
    //        },
    //        "status": 400
    //    }
    {
        std::string json = R"({"error": {"root_cause": [{"type": "parsing_exception","reason":
                           "Unknown key for a VALUE_STRING in [_soure].","line": 1,"col": 128}],
                           "type": "parsing_exception","reason": "Unknown key for a VALUE_STRING
                           in [_soure].","line": 1,"col": 128},"status": 400})";
        HostAddr localHost_{"127.0.0.1", 9200};
        HttpClient hc(localHost_);
        std::vector<std::string> rows;
        auto ret = ESGraphAdapter().result(json, rows);
        ASSERT_FALSE(ret);
    }
}

TEST(FulltextPluginTest, ESPrefixTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    DocItem item("index1", "col1", 1, 2, "aa");
    LimitItem limit(10, 100);
    std::string cmd = ESGraphAdapter().header(client, item, limit) +
                      ESGraphAdapter().body(item, limit.maxRows_, FT_SEARCH_OP::kPrefix);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                           "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\" "
                           "-d'{\"query\":{\"bool\":{\"must\":[{\"term\":{\"schema_id\":2}},"
                           "{\"term\":{\"column_id\":\"8c43de7b01bca674276c43e09b3ec5ba\"}},"
                           "{\"prefix\":{\"value\":\"aa\"}}]}},\"from\":0,\"size\":100,"
                           "\"_source\":\"value\"}'";
    ASSERT_EQ(expected , cmd);
}

TEST(FulltextPluginTest, ESWildcardTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    DocItem item("index1", "col1", 1, 2, "a?a");
    LimitItem limit(10, 100);
    std::string cmd = ESGraphAdapter().header(client, item, limit) +
                      ESGraphAdapter().body(item, limit.maxRows_, FT_SEARCH_OP::kWildcard);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                           "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\" "
                           "-d'{\"query\":{\"bool\":{\"must\":[{\"term\":{\"schema_id\":2}},"
                           "{\"term\":{\"column_id\":\"8c43de7b01bca674276c43e09b3ec5ba\"}},"
                           "{\"wildcard\":{\"value\":\"a?a\"}}]}},\"from\":0,\"size\":100,"
                           "\"_source\":\"value\"}'";
    ASSERT_EQ(expected , cmd);
}

TEST(FulltextPluginTest, ESRegexpTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    DocItem item("index1", "col1", 1, 2, "+a");
    LimitItem limit(10, 100);
    std::string cmd = ESGraphAdapter().header(client, item, limit) +
                      ESGraphAdapter().body(item, limit.maxRows_, FT_SEARCH_OP::kRegexp);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                           "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\" "
                           "-d'{\"query\":{\"bool\":{\"must\":[{\"term\":{\"schema_id\":2}},"
                           "{\"term\":{\"column_id\":\"8c43de7b01bca674276c43e09b3ec5ba\"}},"
                           "{\"regexp\":{\"value\":\"+a\"}}]}},\"from\":0,\"size\":100,"
                           "\"_source\":\"value\"}'";
    ASSERT_EQ(expected , cmd);
}

TEST(FulltextPluginTest, ESFuzzyTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    DocItem item("index1", "col1", 1, 2, "+a");
    LimitItem limit(10, 100);
    {
        std::string cmd = ESGraphAdapter().header(client, item, limit) +
                          ESGraphAdapter().body(item,
                                                limit.maxRows_,
                                                FT_SEARCH_OP::kFuzzy,
                                                "AUTO",
                                                "and");
        std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                               "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\" "
                               "-d'{\"query\":{\"bool\":{\"must\":[{\"term\":{\"schema_id\":2}},"
                               "{\"term\":{\"column_id\":\"8c43de7b01bca674276c43e09b3ec5ba\"}},"
                               "{\"match\":{\"value\":{\"operator\":\"and\",\"query\":\"+a\","
                               "\"fuzziness\":\"AUTO\"}}}]}},"
                               "\"from\":0,\"size\":100,\"_source\":\"value\"}'";
        ASSERT_EQ(expected , cmd);
    }
    {
        std::string cmd = ESGraphAdapter().header(client, item, limit) +
                          ESGraphAdapter().body(item,
                                                limit.maxRows_,
                                                FT_SEARCH_OP::kFuzzy,
                                                2,
                                                "and");
        std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                               "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\" "
                               "-d'{\"query\":{\"bool\":{\"must\":[{\"term\":{\"schema_id\":2}},"
                               "{\"term\":{\"column_id\":\"8c43de7b01bca674276c43e09b3ec5ba\"}},"
                               "{\"match\":{\"value\":{\"operator\":\"and\",\"query\":\"+a\","
                               "\"fuzziness\":2}}}]}},"
                               "\"from\":0,\"size\":100,\"_source\":\"value\"}'";
        ASSERT_EQ(expected , cmd);
    }
}

std::string esJsonDoc = R"({"query":{"bool":{"must":[{"match":{"schema_id":2}},{"match":{"column_id":"col2"}},{"prefix":{"value":"c"}}]}},"from":0,"size":1})";  // NOLINT

folly::dynamic mockJson() {
    //    {
    //        "query": {
    //            "bool": {
    //                "must": [
    //                {
    //                    "match": {
    //                        "schema_id": 2
    //                    }
    //                },
    //                {
    //                    "match": {
    //                        "column_id": "col2"
    //                    }
    //                },
    //                {
    //                    "prefix": {
    //                        "value": "c"
    //                    }
    //                }
    //                ]
    //            }
    //        },
    //        "from": 0,
    //        "size": 1
    //    }

    folly::dynamic itemValue = folly::dynamic::object("value", "c");
    folly::dynamic itemPrefix = folly::dynamic::object("prefix", itemValue);
    folly::dynamic itemColumn = folly::dynamic::object("column_id", "col2");
    folly::dynamic itemMatchCol = folly::dynamic::object("match", itemColumn);
    folly::dynamic itemTag = folly::dynamic::object("schema_id", 2);
    folly::dynamic itemMatchTag = folly::dynamic::object("match", itemTag);
    auto itemArray = folly::dynamic::array(itemMatchTag, itemMatchCol, itemPrefix);
    folly::dynamic itemMust = folly::dynamic::object("must", itemArray);
    folly::dynamic itemBool = folly::dynamic::object("bool", itemMust);
    folly::dynamic itemQuery = folly::dynamic::object("query", itemBool)("size", 1)("from", 0);
    return itemQuery;
}

TEST(FulltextPluginTest, jsonGenTest) {
    auto str = folly::toJson(mockJson());
    ASSERT_EQ(str, esJsonDoc);
}

}   // namespace plugin
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
