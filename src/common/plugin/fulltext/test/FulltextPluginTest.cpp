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

void verifyBodyStr(const std::string& actual, const folly::dynamic& expect) {
    ASSERT_EQ(" -d'", actual.substr(0, 4));
    ASSERT_EQ("'", actual.substr(actual.size() - 1, 1));
    auto body = folly::parseJson(actual.substr(4, actual.size() - 5));
    ASSERT_EQ(expect, body);
}

void verifyBodyStr(const std::string& actual, const std::vector<folly::dynamic>& expect) {
    std::vector<std::string> lines;
    folly::split("\n", actual, lines, true);
    if (lines.size() > 0) {
        ASSERT_LE(2, lines.size());
        ASSERT_EQ("'", lines[lines.size() - 1]);
        for (size_t i = 1; i < lines.size() - 1; i++) {
            auto body = folly::parseJson(lines[i]);
            ASSERT_EQ(expect[i - 1], body);
        }
    }
}

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
    DocItem item("index1", "col1", 1, "aaaa");
    auto header = ESStorageAdapter().putHeader(hc, item);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                           "-XPUT \"http://127.0.0.1:9200/index1/_doc/"
                           "00000000018c43de7b01bca674276c43e09b3ec5baYWFhYQ==\"";
    ASSERT_EQ(expected, header);

    auto body = ESStorageAdapter().putBody(item);

    folly::dynamic d = folly::dynamic::object("column_id", DocIDTraits::column(item.column))
                                             ("value", DocIDTraits::val(item.val));
    verifyBodyStr(std::move(body), std::move(d));
}

TEST(FulltextPluginTest, ESBulkTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient hc(localHost_);
    DocItem item1("index1", "col1", 1, "aaaa");
    DocItem item2("index1", "col1", 1, "bbbb");
    std::set<std::string> strs;
    strs.emplace("aaa");
    strs.emplace("bbb");
    std::vector<DocItem> items;
    items.emplace_back(DocItem("index1", "col1", 1, "aaaa"));
    items.emplace_back(DocItem("index1", "col1", 1, "bbbb"));
    auto header = ESStorageAdapter().bulkHeader(hc);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/x-ndjson; "
                           "charset=utf-8\" -XPOST \"http://127.0.0.1:9200/_bulk\"";
    ASSERT_EQ(expected, header);

    std::vector<folly::dynamic> bodys;
    for (const auto& item : items) {
        folly::dynamic meta = folly::dynamic::object("_id", DocIDTraits::docId(item))
                                                    ("_index", item.index);
        folly::dynamic data = folly::dynamic::object("value", DocIDTraits::val(item.val))
                                                    ("column_id", DocIDTraits::column(item.column));
        bodys.emplace_back(folly::dynamic::object("index", std::move(meta)));
        bodys.emplace_back(std::move(data));
    }

    auto body = ESStorageAdapter().bulkBody(items);
    verifyBodyStr(body, std::move(bodys));
}

TEST(FulltextPluginTest, ESPutToTest) {
    HostAddr localHost_{"127.0.0.1", 11111};
    HttpClient hc(localHost_);
    DocItem item1("index1", "col1", 1, "aaaa");
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

// TODO: The json string is not comparable.
TEST(FulltextPluginTest, ESPrefixTest) {
    HostAddr localHost_{"127.0.0.1", 9200};
    HttpClient client(localHost_);
    DocItem item("index1", "col1", 1, "aa");
    LimitItem limit(10, 100);
    auto header = ESGraphAdapter().header(client, item, limit);
    std::string expected = "/usr/bin/curl -H \"Content-Type: application/json; charset=utf-8\" "
                           "-XGET \"http://127.0.0.1:9200/index1/_search?timeout=10ms\"";
    ASSERT_EQ(expected, header);

    auto body = ESGraphAdapter().prefixBody("aa");
    ASSERT_EQ("{\"prefix\":{\"value\":\"aa\"}}", folly::toJson(body));
}

TEST(FulltextPluginTest, ESWildcardTest) {
    auto body = ESGraphAdapter().wildcardBody("a?a");
    ASSERT_EQ("{\"wildcard\":{\"value\":\"a?a\"}}", folly::toJson(body));
}

TEST(FulltextPluginTest, ESRegexpTest) {
    auto body = ESGraphAdapter().regexpBody("+a");
    ASSERT_EQ("{\"regexp\":{\"value\":\"+a\"}}", folly::toJson(body));
}

TEST(FulltextPluginTest, ESFuzzyTest) {
    auto body = ESGraphAdapter().fuzzyBody("+a", "AUTO", "OR");
    auto expected = "{\"match\":{\"value\":{\"operator\":\"OR\","
                    "\"query\":\"+a\",\"fuzziness\":\"AUTO\"}}}";
    ASSERT_EQ(folly::parseJson(expected), body);
}
}   // namespace plugin
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
