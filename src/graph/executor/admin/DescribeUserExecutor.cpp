/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/DescribeUserExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>    // for max
#include <string>       // for string, basic_string, all...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for DataSet, Row
#include "common/datatypes/Value.h"         // for Value
#include "common/meta/SchemaManager.h"      // for SchemaManager
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Admin.h"       // for DescribeUser
#include "interface/gen-cpp2/meta_types.h"  // for RoleItem

namespace nebula {
namespace graph {

folly::Future<Status> DescribeUserExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return describeUser();
}

folly::Future<Status> DescribeUserExecutor::describeUser() {
  auto* duNode = asNode<DescribeUser>(node());
  return qctx()
      ->getMetaClient()
      ->getUserRoles(*duNode->username())
      .via(runner())
      .thenValue([this](StatusOr<std::vector<meta::cpp2::RoleItem>>&& resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }

        DataSet v({"role", "space"});
        auto roleItemList = std::move(resp).value();
        for (auto& item : roleItemList) {
          if (item.get_space_id() == 0) {
            v.emplace_back(
                nebula::Row({apache::thrift::util::enumNameSafe(item.get_role_type()), ""}));
          } else {
            auto spaceNameResult = qctx_->schemaMng()->toGraphSpaceName(item.get_space_id());
            if (spaceNameResult.ok()) {
              v.emplace_back(nebula::Row({apache::thrift::util::enumNameSafe(item.get_role_type()),
                                          spaceNameResult.value()}));
            } else {
              LOG(ERROR) << " Space name of " << item.get_space_id() << " no found";
              return Status::Error("Space not found");
            }
          }
        }
        return finish(
            ResultBuilder().value(Value(std::move(v))).iter(Iterator::Kind::kSequential).build());
      });
}

}  // namespace graph
}  // namespace nebula
