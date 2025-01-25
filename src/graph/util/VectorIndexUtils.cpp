/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/util/VectorIndexUtils.h"

#include "common/expression/Expression.h"
#include "graph/util/ExpressionUtils.h"

DECLARE_uint32(ft_request_retry_times);

namespace nebula {
namespace graph {

bool VectorIndexUtils::needVectorSearch(const Expression* expr) {
  switch (expr->kind()) {
    case Expression::Kind::kVectorQuery: {
      return true;
    }
    default:
      return false;
  }
}

StatusOr<::nebula::plugin::ESAdapter> VectorIndexUtils::getESAdapter(meta::MetaClient* client) {
  auto tcs = client->getServiceClientsFromCache(meta::cpp2::ExternalServiceType::ELASTICSEARCH);
  if (!tcs.ok()) {
    return tcs.status();
  }
  if (tcs.value().empty()) {
    return Status::SemanticError("No vector search client found");
  }
  std::vector<::nebula::plugin::ESClient> clients;
  for (const auto& c : tcs.value()) {
    std::string protocol = c.conn_type_ref().has_value() ? *c.get_conn_type() : "http";
    std::string address = c.host.toRawString();
    std::string user = c.user_ref().has_value() ? *c.user_ref() : "";
    std::string password = c.pwd_ref().has_value() ? *c.pwd_ref() : "";
    clients.emplace_back(HttpClient::instance(), protocol, address, user, password);
  }
  return ::nebula::plugin::ESAdapter(std::move(clients));
}

}  // namespace graph
}  // namespace nebula 