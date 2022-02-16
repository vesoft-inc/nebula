/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/DropUserExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>, FutureBas...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Promise...
#include <folly/futures/Promise.h>  // for PromiseException::PromiseExc...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Promise...
#include <folly/futures/Promise.h>  // for PromiseException::PromiseExc...

#include "clients/meta/MetaClient.h"     // for MetaClient
#include "common/base/Status.h"          // for Status, NG_RETURN_IF_ERROR
#include "common/base/StatusOr.h"        // for StatusOr
#include "common/time/ScopedTimer.h"     // for SCOPED_TIMER
#include "graph/context/QueryContext.h"  // for QueryContext
#include "graph/planner/plan/Admin.h"    // for DropUser

namespace nebula {
namespace graph {

folly::Future<Status> DropUserExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return dropUser();
}

folly::Future<Status> DropUserExecutor::dropUser() {
  auto *duNode = asNode<DropUser>(node());
  return qctx()
      ->getMetaClient()
      ->dropUser(*duNode->username(), duNode->ifExist())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Drop user failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
