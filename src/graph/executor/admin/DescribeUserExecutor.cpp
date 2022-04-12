// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/DescribeUserExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

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
              LOG(WARNING) << " Space name of " << item.get_space_id()
                           << " no found: " << spaceNameResult.status();
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
