/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <regex>

#include "common/http/HttpClient.h"
#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "common/plugin/fulltext/elasticsearch/ESClient.h"
#include "common/plugin/fulltext/test/ESQueryResultExample.h"
#include "folly/String.h"
#include "gflags/gflags.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace nebula {

using ::testing::_;
using ::testing::Eq;
using ::testing::Return;

class MockHttpClient : public HttpClient {
 public:
  MOCK_METHOD(HttpResponse, get, (const std::string& url), (override));
  MOCK_METHOD(HttpResponse,
              get,
              (const std::string& url, const std::vector<std::string>& headers),
              (override));
  MOCK_METHOD(HttpResponse,
              get,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string&,
               const std::string&),
              (override));
  MOCK_METHOD(HttpResponse,
              post,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string& body),
              (override));
  MOCK_METHOD(HttpResponse,
              post,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string& body,
               const std::string&,
               const std::string&),
              (override));
  MOCK_METHOD(HttpResponse,
              delete_,
              (const std::string& url, const std::vector<std::string>& headers),
              (override));
  MOCK_METHOD(HttpResponse,
              delete_,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string&,
               const std::string&),
              (override));
  MOCK_METHOD(HttpResponse,
              put,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string& body),
              (override));
  MOCK_METHOD(HttpResponse,
              put,
              (const std::string& url,
               const std::vector<std::string>& headers,
               const std::string& body,
               const std::string&,
               const std::string&),
              (override));
};

HttpResponse operator"" _http_resp(const char* s, size_t) {
  HttpResponse resp;
  folly::StringPiece text(s);
  text = folly::trimWhitespace(text);
  std::string curlResult = text.split_step("\n\n").toString();
  std::regex curlReg(R"(curl: \((\d+)\)( (.*))?)");
  std::smatch matchResult;
  CHECK(std::regex_match(curlResult, matchResult, curlReg));
  resp.curlCode = CURLcode(std::stoi(matchResult[1].str()));
  if (matchResult.size() > 2) {
    resp.curlMessage = matchResult[3];
  }
  if (!text.empty()) {
    folly::StringPiece header = text.split_step("\n\n");
    resp.header = header.toString();
  }
  if (!text.empty()) {
    resp.body = text.toString();
  }
  return resp;
}

class ESTest : public ::testing::Test {
 public:
  void SetUp() override {
    queryResultResp_.body = queryResultBody;
  }

 protected:
  HttpResponse queryResultResp_ = R"(
curl: (0)

HTTP/1.1 200 OK
content-type: application/json; charset=UTF-8
content-length: 78

)"_http_resp;
  HttpResponse normalSuccessResp_ = R"(
curl: (0)

HTTP/1.1 200 OK
content-type: application/json; charset=UTF-8
content-length: 78

{"acknowledged":true,"shards_acknowledged":true,"index":"nebula_test_index_1"}
)"_http_resp;

  HttpResponse esErrorResp_ = R"(
curl: (0)

HTTP/1.1 400 Bad Request
content-type: application/json; charset=UTF-8
content-length: 417

{"error":{"reason":"mock error"},"status":400}
)"_http_resp;

  HttpResponse curlErrorResp_ = R"(
curl: (7) mock error message

)"_http_resp;
};

TEST_F(ESTest, createIndex) {
  MockHttpClient mockHttpClient;
  EXPECT_CALL(mockHttpClient,
              put("http://127.0.0.1:9200/nebula_index_1",
                  std::vector<std::string>{"Content-Type: application/json"},
                  _,
                  "",
                  ""))
      .Times(3)
      .WillOnce(Return(normalSuccessResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  std::vector<plugin::ESClient> clients;
  clients.push_back(client);
  plugin::ESAdapter adapter(std::move(clients));
  {
    auto result = adapter.createIndex("nebula_index_1");
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.createIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.createIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, dropIndex) {
  MockHttpClient mockHttpClient;
  EXPECT_CALL(mockHttpClient,
              delete_("http://127.0.0.1:9200/nebula_index_1",
                      std::vector<std::string>{"Content-Type: application/json"},
                      "",
                      ""))
      .Times(3)
      .WillOnce(Return(normalSuccessResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.dropIndex("nebula_index_1");
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.dropIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.dropIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, getIndex) {
  MockHttpClient mockHttpClient;
  HttpResponse indexExistResp = R"(
curl: (0)

HTTP/1.1 200 OK
content-type: application/json; charset=UTF-8
content-length: 78

{"nebula_index_1":{}}
)"_http_resp;

  HttpResponse indexNotExistResp = R"(
curl: (0)

HTTP/1.1 404 OK
content-type: application/json; charset=UTF-8
content-length: 78

{"error":{"reason":"mock error"},"status":404}
)"_http_resp;

  EXPECT_CALL(mockHttpClient,
              get("http://127.0.0.1:9200/nebula_index_1",
                  std::vector<std::string>{"Content-Type: application/json"},
                  "",
                  ""))
      .Times(4)
      .WillOnce(Return(indexExistResp))
      .WillOnce(Return(indexNotExistResp))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.isIndexExist("nebula_index_1");
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value());
  }
  {
    auto result = adapter.isIndexExist("nebula_index_1");
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value());
  }
  {
    auto result = adapter.isIndexExist("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.isIndexExist("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, clearIndex) {
  MockHttpClient mockHttpClient;
  auto clearSuccessResp_ = R"(
curl: (0)

HTTP/1.1 200 OK
content-type: application/json; charset=UTF-8
content-length: 78

{"failures":[]}
)"_http_resp;
  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_delete_by_query?refresh=false",
                   std::vector<std::string>{"Content-Type: application/json"},
                   _,
                   "",
                   ""))
      .Times(2)
      .WillOnce(Return(clearSuccessResp_))
      .WillOnce(Return(esErrorResp_));
  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_delete_by_query?refresh=true",
                   std::vector<std::string>{"Content-Type: application/json"},
                   _,
                   "",
                   ""))
      .Times(1)
      .WillOnce(Return(curlErrorResp_));

  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.clearIndex("nebula_index_1");
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.clearIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.clearIndex("nebula_index_1", true);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, prefix) {
  folly::dynamic prefixBody = folly::parseJson(R"(
    {
      "query":{
        "prefix":{
            "text":"abc"
        }
      }
    }
   )");
  MockHttpClient mockHttpClient;

  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_search",
                   std::vector<std::string>{"Content-Type: application/json"},
                   folly::toJson(prefixBody),
                   std::string(""),
                   std::string("")))
      .Times(3)
      .WillOnce(Return(queryResultResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.prefix("nebula_index_1", "abc", -1, -1);
    ASSERT_TRUE(result.ok());
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "vertex text"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("a", "b", 10, "edge text"));
  }
  {
    auto result = adapter.prefix("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.prefix("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, wildcard) {
  folly::dynamic body = folly::parseJson(R"(
    {
      "query":{
        "wildcard":{
            "text":"abc"
        }
      }
    }
   )");
  MockHttpClient mockHttpClient;

  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_search",
                   std::vector<std::string>{"Content-Type: application/json"},
                   folly::toJson(body),
                   "",
                   ""))
      .Times(3)
      .WillOnce(Return(queryResultResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.wildcard("nebula_index_1", "abc", -1, -1);
    ASSERT_TRUE(result.ok());
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "vertex text"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("a", "b", 10, "edge text"));
  }
  {
    auto result = adapter.wildcard("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.wildcard("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, regexp) {
  folly::dynamic body = folly::parseJson(R"(
    {
      "query":{
        "regexp":{
            "text":"abc"
        }
      }
    }
   )");
  MockHttpClient mockHttpClient;

  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_search",
                   std::vector<std::string>{"Content-Type: application/json"},
                   folly::toJson(body),
                   "",
                   ""))
      .Times(3)
      .WillOnce(Return(queryResultResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.regexp("nebula_index_1", "abc", -1, -1);
    ASSERT_TRUE(result.ok());
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "vertex text"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("a", "b", 10, "edge text"));
  }
  {
    auto result = adapter.regexp("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.regexp("nebula_index_1", "abc", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, bulk) {
  MockHttpClient mockHttpClient;

  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/_bulk?refresh=true",
                   std::vector<std::string>{"Content-Type: application/x-ndjson"},
                   _,
                   "",
                   ""))  // TODO(hs.zhang): Matcher
      .Times(2)
      .WillOnce(Return(queryResultResp_))
      .WillOnce(Return(esErrorResp_));
  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/_bulk?refresh=false",
                   std::vector<std::string>{"Content-Type: application/x-ndjson"},
                   _,
                   "",
                   ""))  // TODO(hs.zhang): Matcher
      .Times(1)
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  plugin::ESBulk bulk;
  bulk.put("nebula_index_1", "1", "", "", 0, "vertex text");
  bulk.delete_("nebula_index_2", "", "a", "b", 10);
  {
    auto result = adapter.bulk(bulk, true);
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.bulk(bulk, true);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.bulk(bulk, false);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"(curl error(7):mock error message)");
  }
}

TEST_F(ESTest, fuzzy) {
  // folly::dynamic body = folly::parseJson(R"(
  //   {
  //     "query":{
  //       "fuzzy":{
  //         "text":{
  //           "fuzziness": "2",
  //           "value": "abc"
  //         }
  //       }
  //     }
  //   }
  //  )");
  MockHttpClient mockHttpClient;

  EXPECT_CALL(mockHttpClient,
              post("http://127.0.0.1:9200/nebula_index_1/_search",
                   std::vector<std::string>{"Content-Type: application/json"},
                   _,
                   "",
                   ""))
      .Times(3)
      .WillOnce(Return(queryResultResp_))
      .WillOnce(Return(esErrorResp_))
      .WillOnce(Return(curlErrorResp_));
  plugin::ESClient client(mockHttpClient, "http", "127.0.0.1:9200", "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.fuzzy("nebula_index_1", "abc", "2", -1, -1);
    ASSERT_TRUE(result.ok());
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "vertex text"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("a", "b", 10, "edge text"));
  }
  {
    auto result = adapter.fuzzy("nebula_index_1", "abc", "2", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.fuzzy("nebula_index_1", "abc", "2", -1, -1);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), R"(curl error(7):mock error message)");
  }
}

DEFINE_string(es_address, "192.168.8.211:9200", "address of elasticsearch");
class RealESTest : public ::testing::Test {};

TEST_F(RealESTest, DISABLED_CREATE_DROP_INDEX) {
  plugin::ESClient client(HttpClient::instance(), "http", FLAGS_es_address, "", "");
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.createIndex("nebula_index_1");
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.createIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.message().find("resource_already_exists_exception"), std::string::npos);
  }
  {
    auto result = adapter.dropIndex("nebula_index_1");
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.dropIndex("nebula_index_1");
    ASSERT_FALSE(result.ok());
    ASSERT_NE(result.message().find("index_not_found_exception"), std::string::npos);
  }
}

TEST_F(RealESTest, DISABLED_QUERY) {
  plugin::ESClient client(HttpClient::instance(), "http", FLAGS_es_address, "", "");
  std::string indexName = "nebula_index_2";
  plugin::ESAdapter adapter(std::vector<plugin::ESClient>({client}));
  {
    auto result = adapter.createIndex(indexName);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    plugin::ESBulk bulk;
    bulk.put(indexName, "1", "", "", 0, "abc");
    bulk.put(indexName, "2", "", "", 0, "abcd");
    auto result = adapter.bulk(bulk, true);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.prefix(indexName, "a", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "abc"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("2", "abcd"));
  }
  {
    auto result = adapter.regexp(indexName, "a.*", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "abc"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("2", "abcd"));
  }
  {
    auto result = adapter.prefix(indexName, "abcd", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 1);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("2", "abcd"));
  }
  {
    plugin::ESBulk bulk;
    bulk.put(indexName, "2", "", "", 0, "NebulaGraph is a graph database");
    bulk.put(indexName, "3", "", "", 0, "The best graph database is NebulaGraph");
    bulk.put(indexName, "4", "", "", 0, "Nebulagraph是最好的图数据库");
    bulk.put(indexName, "5", "", "", 0, "NebulaGraph是一个图数据库");
    auto result = adapter.bulk(bulk, true);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.match_all(indexName);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 5);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("1", "abc"));
    ASSERT_EQ(esResult.items[1],
              plugin::ESQueryResult::Item("2", "NebulaGraph is a graph database"));
    ASSERT_EQ(esResult.items[2],
              plugin::ESQueryResult::Item("3", "The best graph database is NebulaGraph"));
    ASSERT_EQ(esResult.items[3], plugin::ESQueryResult::Item("4", "Nebulagraph是最好的图数据库"));
    ASSERT_EQ(esResult.items[4], plugin::ESQueryResult::Item("5", "NebulaGraph是一个图数据库"));
  }
  {
    auto result = adapter.prefix(indexName, "NebulaGraph", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0],
              plugin::ESQueryResult::Item("2", "NebulaGraph is a graph database"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("5", "NebulaGraph是一个图数据库"));
  }
  {
    auto result = adapter.regexp(indexName, "NebulaGraph.*(图数据库|database)", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0],
              plugin::ESQueryResult::Item("2", "NebulaGraph is a graph database"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("5", "NebulaGraph是一个图数据库"));
  }
  {
    auto result = adapter.wildcard(indexName, "Nebula?raph是*", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 2);
    std::sort(esResult.items.begin(),
              esResult.items.end(),
              [](const plugin::ESQueryResult::Item& a, const plugin::ESQueryResult::Item& b) {
                return a.vid < b.vid;
              });
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("4", "Nebulagraph是最好的图数据库"));
    ASSERT_EQ(esResult.items[1], plugin::ESQueryResult::Item("5", "NebulaGraph是一个图数据库"));
  }
  {
    auto result = adapter.fuzzy(indexName, "Nebulagraph is a graph Database", "2", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 1);

    ASSERT_EQ(esResult.items[0],
              plugin::ESQueryResult::Item("2", "NebulaGraph is a graph database"));
  }
  {
    plugin::ESBulk bulk;
    bulk.delete_(indexName, "2", "", "", 0);
    auto result = adapter.bulk(bulk, true);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.prefix(indexName, "NebulaGraph", -1, -1);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 1);
    ASSERT_EQ(esResult.items[0], plugin::ESQueryResult::Item("5", "NebulaGraph是一个图数据库"));
  }
  {
    auto result = adapter.clearIndex(indexName, true);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.match_all(indexName);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto esResult = std::move(result).value();
    ASSERT_EQ(esResult.items.size(), 0);
  }
  {
    auto result = adapter.isIndexExist(indexName);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_TRUE(result.value());
  }
  {
    auto result = adapter.dropIndex(indexName);
    ASSERT_TRUE(result.ok()) << result.message();
  }
  {
    auto result = adapter.isIndexExist(indexName);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_FALSE(result.value());
  }
}

}  // namespace nebula
