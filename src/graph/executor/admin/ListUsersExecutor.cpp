/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ListUsersExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>  // for PromiseException::Promise...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <algorithm>      // for max
#include <string>         // for string, basic_string, all...
#include <type_traits>    // for remove_reference<>::type
#include <unordered_map>  // for unordered_map, _Node_iter...
#include <utility>        // for move

#include "clients/meta/MetaClient.h"     // for MetaClient
#include "common/base/Status.h"          // for Status
#include "common/base/StatusOr.h"        // for StatusOr
#include "common/datatypes/DataSet.h"    // for DataSet, Row
#include "common/datatypes/Value.h"      // for Value
#include "common/time/ScopedTimer.h"     // for SCOPED_TIMER
#include "graph/context/QueryContext.h"  // for QueryContext

namespace nebula {
namespace graph {

folly::Future<Status> ListUsersExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return listUsers();
}

folly::Future<Status> ListUsersExecutor::listUsers() {
  return qctx()->getMetaClient()->listUsers().via(runner()).thenValue(
      [this](StatusOr<std::unordered_map<std::string, std::string>> &&resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }
        nebula::DataSet v({"Account"});
        auto items = std::move(resp).value();
        for (const auto &item : items) {
          v.emplace_back(nebula::Row({
              std::move(item).first,
          }));
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
