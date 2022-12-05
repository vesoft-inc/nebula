/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/plugin/fulltext/elasticsearch/ESClient.h"

#include "common/http/HttpClient.h"
namespace nebula::plugin {

ESClient::ESClient(HttpClient& httpClient,
                   const std::string& protocol,
                   const std::string& address,
                   const std::string& user,
                   const std::string& password)
    : httpClient_(httpClient),
      protocol_(protocol),
      address_(address),
      user_(user),
      password_(password) {
  // TODO(hs.zhang): enable protocol
  // TODO(hs.zhang): enable user&password
}

/**
 * @brief
 *
 * @param name
 * @param body
 * @return
 *
 * Success
 * {
 *   "acknowledged":true,
 *   "shards_acknowledged":true,
 *   "index":"nebula_test_index_1"
 * }
 *
 * Failed
 * {
 *   "error":{
 *       "root_cause":[
 *           {
 *               "type":...,
 *               "reason":...,
 *               "index_uuid":...,
 *               "index":...
 *           }
 *       ],
 *       "type":...,
 *       "reason":...,
 *       "index_uuid":...,
 *       "index":...
 *   },
 *   "status":...
 * }
 */
StatusOr<folly::dynamic> ESClient::createIndex(const std::string& name,
                                               const folly::dynamic& body) {
  std::string url = fmt::format("http://{}/{}", address_, name);
  auto resp = httpClient_.put(url, {"Content-Type: application/json"}, folly::toJson(body));
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  auto ret = folly::parseJson(resp.body);
  return ret;
}

StatusOr<folly::dynamic> ESClient::dropIndex(const std::string& name) {
  std::string url = fmt::format("http://{}/{}", address_, name);
  auto resp = httpClient_.delete_(url, {"Content-Type: application/json"});
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::getIndex(const std::string& name) {
  std::string url = fmt::format("http://{}/{}", address_, name);
  auto resp = httpClient_.get(url, {"Content-Type: application/json"});
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::deleteByQuery(const std::string& index,
                                                 const folly::dynamic& query,
                                                 bool refresh) {
  std::string url = fmt::format(
      "http://{}/{}/_delete_by_query?refresh={}", address_, index, refresh ? "true" : "false");
  auto resp = httpClient_.post(url, {"Content-Type: application/json"}, folly::toJson(query));
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::search(const std::string& index,
                                          const folly::dynamic& query,
                                          int64_t timeout) {
  std::string url = fmt::format("http://{}/{}/_search", address_, index);
  if (timeout > 0) {
    url += fmt::format("?timeout={}ms", timeout);
  }
  auto resp = httpClient_.post(url, {"Content-Type: application/json"}, folly::toJson(query));
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::bulk(const std::vector<folly::dynamic>& bulk, bool refresh) {
  std::string url = fmt::format("http://{}/_bulk?refresh={}", address_, refresh ? "true" : "false");
  std::string body;
  for (auto& obj : bulk) {
    body += folly::toJson(obj);
    body += "\n";
  }
  auto resp = httpClient_.post(url, {"Content-Type: application/x-ndjson"}, body);
  if (resp.curlCode != 0) {
    return Status::Error(fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage));
  }
  return folly::parseJson(resp.body);
}

}  // namespace nebula::plugin
