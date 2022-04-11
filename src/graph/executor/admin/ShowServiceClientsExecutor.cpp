// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/ShowServiceClientsExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/planner/plan/Admin.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowServiceClientsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return showServiceClients();
}

folly::Future<Status> ShowServiceClientsExecutor::showServiceClients() {
  auto *siNode = asNode<ShowServiceClients>(node());
  auto type = siNode->type();

  return qctx()->getMetaClient()->listServiceClients(type).via(runner()).thenValue(
      [this](auto &&resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Show service client fail: " << resp.status();
          return resp.status();
        }
        auto values = std::move(resp).value();
        DataSet v({"Type", "Host", "Port"});
        for (const auto &value : values) {
          for (const auto &client : value.second) {
            nebula::Row r({apache::thrift::util::enumNameSafe(value.first),
                           client.host.host,
                           client.host.port});
            v.emplace_back(std::move(r));
          }
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
