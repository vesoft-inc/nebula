/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/IngestExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>, FutureB...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Promi...
#include <folly/futures/Promise.h>  // for PromiseException::PromiseE...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Promi...
#include <folly/futures/Promise.h>  // for PromiseException::PromiseE...

#include "clients/meta/MetaClient.h"       // for MetaClient
#include "common/base/Status.h"            // for Status, NG_RETURN_IF_ERROR
#include "common/base/StatusOr.h"          // for StatusOr
#include "common/time/ScopedTimer.h"       // for SCOPED_TIMER
#include "graph/context/QueryContext.h"    // for QueryContext
#include "graph/service/RequestContext.h"  // for RequestContext
#include "graph/session/ClientSession.h"   // for ClientSession, SpaceInfo

namespace nebula {
namespace graph {

folly::Future<Status> IngestExecutor::execute() {
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->ingest(spaceId).via(runner()).thenValue(
      [this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Ingest failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
