/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowServiceClientsExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>      // for max
#include <string>         // for string, basic_string, all...
#include <unordered_map>  // for unordered_map
#include <utility>        // for move
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"     // for ServiceClientsList, MetaC...
#include "common/base/Logging.h"         // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"          // for Status, operator<<
#include "common/base/StatusOr.h"        // for StatusOr
#include "common/datatypes/DataSet.h"    // for DataSet, Row
#include "common/datatypes/List.h"       // for List
#include "common/time/ScopedTimer.h"     // for SCOPED_TIMER
#include "graph/context/QueryContext.h"  // for QueryContext
#include "graph/planner/plan/Admin.h"    // for ShowServiceClients

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
          LOG(ERROR) << resp.status();
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
