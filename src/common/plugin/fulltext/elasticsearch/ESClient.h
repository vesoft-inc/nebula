/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_CLIENT_
#define COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_CLIENT_
#include <folly/json.h>

#include <string>
#include <vector>

#include "common/base/StatusOr.h"
#include "common/http/HttpClient.h"
namespace nebula::plugin {

class ESClient {
 public:
  ESClient(const ESClient& client) = default;
  ESClient(HttpClient& httpClient,
           const std::string& protocol,
           const std::string& address,
           const std::string& user,
           const std::string& password);
  ESClient& operator=(const ESClient& client) {
    if (&client == this) {
      return *this;
    }
    protocol_ = client.protocol_;
    httpClient_ = client.httpClient_;
    address_ = client.address_;
    username_ = client.username_;
    password_ = client.password_;
    return *this;
  }
  StatusOr<folly::dynamic> createIndex(const std::string& name, const folly::dynamic& object);
  StatusOr<folly::dynamic> dropIndex(const std::string& name);

  StatusOr<folly::dynamic> getIndex(const std::string& name);

  StatusOr<folly::dynamic> deleteByQuery(const std::string& index,
                                         const folly::dynamic& query,
                                         bool refresh = false);
  StatusOr<folly::dynamic> search(const std::string& index,
                                  const folly::dynamic& query,
                                  int64_t timeout);
  StatusOr<folly::dynamic> bulk(const std::vector<folly::dynamic>& bulk, bool refresh = false);

 private:
  HttpClient& httpClient_;
  std::string protocol_;
  std::string address_;
  std::string username_;
  std::string password_;

  StatusOr<folly::dynamic> sendHttpRequest(const std::string& url,
                                           const std::string& method,
                                           const std::vector<std::string>& header,
                                           const std::string& body);
};

}  // namespace nebula::plugin

#endif
