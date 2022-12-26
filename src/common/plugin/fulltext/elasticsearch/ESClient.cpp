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
      username_(user),
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
  std::string url = fmt::format("{}://{}/{}", protocol_, address_, name);
  auto resp = httpClient_.put(
      url, {"Content-Type: application/json"}, folly::toJson(body), username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  auto ret = folly::parseJson(resp.body);
  return ret;
}

StatusOr<folly::dynamic> ESClient::dropIndex(const std::string& name) {
  std::string url = fmt::format("{}://{}/{}", protocol_, address_, name);
  auto resp = httpClient_.delete_(url, {"Content-Type: application/json"}, username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::getIndex(const std::string& name) {
  std::string url = fmt::format("{}://{}/{}", protocol_, address_, name);
  auto resp = httpClient_.get(url, {"Content-Type: application/json"}, username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::deleteByQuery(const std::string& index,
                                                 const folly::dynamic& query,
                                                 bool refresh) {
  std::string url = fmt::format("{}://{}/{}/_delete_by_query?refresh={}",
                                protocol_,
                                address_,
                                index,
                                refresh ? "true" : "false");
  auto resp = httpClient_.post(
      url, {"Content-Type: application/json"}, folly::toJson(query), username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::search(const std::string& index,
                                          const folly::dynamic& query,
                                          int64_t timeout) {
  std::string url = fmt::format("{}://{}/{}/_search", protocol_, address_, index);
  if (timeout > 0) {
    url += fmt::format("?timeout={}ms", timeout);
  }
  auto resp = httpClient_.post(
      url, {"Content-Type: application/json"}, folly::toJson(query), username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  return folly::parseJson(resp.body);
}

StatusOr<folly::dynamic> ESClient::bulk(const std::vector<folly::dynamic>& bulk, bool refresh) {
  std::string url =
      fmt::format("{}://{}/_bulk?refresh={}", protocol_, address_, refresh ? "true" : "false");
  std::string body;
  for (auto& obj : bulk) {
    body += folly::toJson(obj);
    body += "\n";
  }
  auto resp =
      httpClient_.post(url, {"Content-Type: application/x-ndjson"}, body, username_, password_);
  if (resp.curlCode != 0) {
    std::string msg = fmt::format("curl error({}):{}", resp.curlCode, resp.curlMessage);
    LOG(ERROR) << msg;
    return Status::Error(msg);
  }
  return folly::parseJson(resp.body);
}

}  // namespace nebula::plugin
