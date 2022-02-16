/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SwitchSpaceExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>, Futu...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <ostream>  // for operator<<, basic_ostream
#include <string>   // for operator<<, char_traits
#include <utility>  // for move

#include "clients/meta/MetaClient.h"          // for MetaClient
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG_E...
#include "common/base/Status.h"               // for Status, operator<<, NG_...
#include "common/base/StatusOr.h"             // for StatusOr
#include "common/time/ScopedTimer.h"          // for SCOPED_TIMER
#include "graph/context/QueryContext.h"       // for QueryContext
#include "graph/planner/plan/Query.h"         // for SwitchSpace
#include "graph/service/PermissionManager.h"  // for PermissionManager
#include "graph/service/RequestContext.h"     // for RequestContext
#include "graph/session/ClientSession.h"      // for SpaceInfo, ClientSession
#include "interface/gen-cpp2/meta_types.h"    // for SpaceItem, SpaceDesc

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *spaceToNode = asNode<SwitchSpace>(node());
  auto spaceName = spaceToNode->getSpaceName();
  return qctx()->getMetaClient()->getSpace(spaceName).via(runner()).thenValue(
      [spaceName, this](StatusOr<meta::cpp2::SpaceItem> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }

        auto spaceId = resp.value().get_space_id();
        auto *session = qctx_->rctx()->session();
        NG_RETURN_IF_ERROR(PermissionManager::canReadSpace(session, spaceId));
        const auto &properties = resp.value().get_properties();

        SpaceInfo spaceInfo;
        spaceInfo.id = spaceId;
        spaceInfo.name = spaceName;
        spaceInfo.spaceDesc = std::move(properties);
        qctx_->rctx()->session()->setSpace(std::move(spaceInfo));
        LOG(INFO) << "Graph switched to `" << spaceName << "', space id: " << spaceId;
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
