/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/http/HttpClient.h"
#include "gtest/gtest.h"
#include "mock/FakeHttpServer.h"

namespace nebula {

class HTTPClientTest : public ::testing::Test {};

TEST_F(HTTPClientTest, GET) {
  FakeHttpServer server(3659);
  server.start();
  auto resp = HttpClient::instance().get("http://localhost:3659");
  ASSERT_EQ(resp.curlCode, 0) << resp.curlMessage;
  ASSERT_EQ(resp.body, "GET");
  server.stop();
  server.join();
}

TEST_F(HTTPClientTest, POST) {
  FakeHttpServer server(3660);
  server.start();
  auto resp = HttpClient::instance().post("http://localhost:3660", {}, "");
  ASSERT_EQ(resp.curlCode, 0) << resp.curlMessage;
  ASSERT_EQ(resp.body, "POST");
  server.stop();
  server.join();
}

TEST_F(HTTPClientTest, DELETE) {
  FakeHttpServer server(3661);
  server.start();
  auto resp = HttpClient::instance().delete_("http://localhost:3661", {});
  ASSERT_EQ(resp.curlCode, 0) << resp.curlMessage;
  ASSERT_EQ(resp.body, "DELETE");
  server.stop();
  server.join();
}

TEST_F(HTTPClientTest, PUT) {
  FakeHttpServer server(3662);
  server.start();
  auto resp = HttpClient::instance().put("http://localhost:3662", {}, "");
  ASSERT_EQ(resp.curlCode, 0) << resp.curlMessage;
  ASSERT_EQ(resp.body, "PUT");
  server.stop();
  server.join();
}

}  // namespace nebula
