/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/DescribeUserExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "interface/gen-cpp2/meta_types.h"

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
      ->describeUser(*duNode->username())
      .via(runner())
      .thenValue([this](StatusOr<meta::cpp2::UserDescItem>&& resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }

        DataSet v({"role", "space"});
        auto user = std::move(resp).value();
        auto spaceRoleMap = user.get_space_role_map();
        std::vector<std::string> rolesInSpacesStrVector;
        for (auto& item : spaceRoleMap) {
          auto spaceNameResult = qctx_->schemaMng()->toGraphSpaceName(item.first);
          if (!spaceNameResult.ok()) {
            if (item.first == 0) {
              v.emplace_back(nebula::Row({apache::thrift::util::enumNameSafe(item.second), ""}));
            } else {
              LOG(ERROR) << " Space name of " << item.first << " no found";
              return Status::Error("Space not found");
            }
          } else {
            v.emplace_back(nebula::Row(
                {apache::thrift::util::enumNameSafe(item.second), spaceNameResult.value()}));
          }
        }
        return finish(
            ResultBuilder().value(Value(std::move(v))).iter(Iterator::Kind::kSequential).build());
      });
}

}  // namespace graph
}  // namespace nebula
