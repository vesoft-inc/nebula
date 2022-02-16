/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ChangePasswordExecutor.h"

#include <folly/Range.h>                           // for Range
#include <folly/Try.h>                             // for Try::~Try<T>
#include <folly/futures/Future.h>                  // for Future::Future<T>
#include <folly/futures/Promise.h>                 // for Promise::Promise<T>
#include <folly/futures/Promise.h>                 // for PromiseException::...
#include <folly/futures/Promise.h>                 // for Promise::Promise<T>
#include <folly/futures/Promise.h>                 // for PromiseException::...
#include <folly/io/async/ScopedEventBaseThread.h>  // for StringPiece
#include <proxygen/lib/utils/CryptUtil.h>          // for md5Encode

#include "clients/meta/MetaClient.h"     // for MetaClient
#include "common/base/Status.h"          // for Status, NG_RETURN_...
#include "common/base/StatusOr.h"        // for StatusOr
#include "common/time/ScopedTimer.h"     // for SCOPED_TIMER
#include "graph/context/QueryContext.h"  // for QueryContext
#include "graph/planner/plan/Admin.h"    // for ChangePassword

namespace nebula {
namespace graph {

folly::Future<Status> ChangePasswordExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return changePassword();
}

folly::Future<Status> ChangePasswordExecutor::changePassword() {
  auto *cpNode = asNode<ChangePassword>(node());
  return qctx()
      ->getMetaClient()
      ->changePassword(*cpNode->username(),
                       proxygen::md5Encode(folly::StringPiece(*cpNode->newPassword())),
                       proxygen::md5Encode(folly::StringPiece(*cpNode->password())))
      .via(runner())
      .thenValue([this](StatusOr<bool> &&resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Change password failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
