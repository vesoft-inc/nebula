/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ListenerExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for operator<, operator!=

#include <algorithm>    // for max, sort
#include <string>       // for string, basic_string, all...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for operator<<, Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/datatypes/List.h"          // for List
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/planner/plan/Admin.h"       // for AddListener, RemoveListener
#include "graph/service/RequestContext.h"   // for RequestContext
#include "graph/session/ClientSession.h"    // for ClientSession, SpaceInfo
#include "interface/gen-cpp2/meta_types.h"  // for ListenerInfo, swap

namespace nebula {
namespace graph {

folly::Future<Status> AddListenerExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* alNode = asNode<AddListener>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->addListener(spaceId, alNode->type(), alNode->hosts())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> RemoveListenerExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* rlNode = asNode<RemoveListener>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->removeListener(spaceId, rlNode->type())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> ShowListenerExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listListener(spaceId).via(runner()).thenValue(
      [this](StatusOr<std::vector<meta::cpp2::ListenerInfo>> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }

        auto listenerInfos = std::move(resp).value();
        std::sort(listenerInfos.begin(), listenerInfos.end(), [](const auto& a, const auto& b) {
          if (a.part_id_ref() != b.part_id_ref()) {
            return a.part_id_ref() < b.part_id_ref();
          }
          if (a.type_ref() != b.type_ref()) {
            return a.type_ref() < b.type_ref();
          }
          return a.host_ref() < b.host_ref();
        });

        DataSet result({"PartId", "Type", "Host", "Status"});
        for (const auto& info : listenerInfos) {
          Row row;
          row.values.emplace_back(info.get_part_id());
          row.values.emplace_back(apache::thrift::util::enumNameSafe(info.get_type()));
          row.values.emplace_back((*info.host_ref()).toString());
          row.values.emplace_back(apache::thrift::util::enumNameSafe(info.get_status()));
          result.emplace_back(std::move(row));
        }

        return finish(std::move(result));
      });
}

}  // namespace graph
}  // namespace nebula
