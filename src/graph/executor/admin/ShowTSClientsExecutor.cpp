/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowTSClientsExecutor.h"

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowTSClientsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return showTSClients();
}

folly::Future<Status> ShowTSClientsExecutor::showTSClients() {
  return qctx()->getMetaClient()->listFTClients().via(runner()).thenValue([this](auto&& resp) {
    if (!resp.ok()) {
      LOG(ERROR) << resp.status();
      return resp.status();
    }
    auto value = std::move(resp).value();
    DataSet v({"Host", "Port", "Connection type"});
    for (const auto& client : value) {
      nebula::Row r;
      r.values.emplace_back(client.host.host);
      r.values.emplace_back(client.host.port);
      r.values.emplace_back(client.conn_type_ref().has_value() ? *client.get_conn_type() : "http");
      v.emplace_back(std::move(r));
    }
    return finish(std::move(v));
  });
}

}  // namespace graph
}  // namespace nebula
