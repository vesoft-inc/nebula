/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/RebuildFTJobExecutor.h"

#include <folly/Try.h>              // for Try, Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcep...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcep...

#include "common/base/Logging.h"                // for CHECK, COMPACT_GOOGLE...
#include "common/base/Status.h"                 // for Status
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/datatypes/HostAddr.h"          // for HostAddr
#include "interface/gen-cpp2/meta_types.h"      // for AdminCmd, AdminCmd::R...
#include "meta/processors/admin/AdminClient.h"  // for AdminClient

namespace nebula {
namespace meta {

folly::Future<Status> RebuildFTJobExecutor::executeInternal(HostAddr&& address,
                                                            std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::AdminCmd::REBUILD_FULLTEXT_INDEX,
                jobId_,
                taskId_++,
                space_,
                std::move(address),
                taskParameters_,
                std::move(parts),
                concurrency_)
      .then([pro = std::move(pro)](auto&& t) mutable {
        CHECK(!t.hasException());
        auto status = std::move(t).value();
        if (status.ok()) {
          pro.setValue(Status::OK());
        } else {
          pro.setValue(status.status());
        }
      });
  return f;
}

}  // namespace meta
}  // namespace nebula
