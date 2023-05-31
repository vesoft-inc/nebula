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
    auto result = adapter.createIndex("nebula_index_1", {"a", "b", "c"}, "");
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = adapter.createIndex("nebula_index_1", {"a", "b", "c"}, "");
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.message(), R"({"reason":"mock error"})");
  }
  {
    auto result = adapter.createIndex("nebula_index_1", {"a", "b", "c"}, "");
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

}  // namespace nebula
