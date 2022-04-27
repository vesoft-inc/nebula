
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Admin.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> CreateSpace::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("ifNotExists", folly::toJson(util::toJson(ifNotExists_)), desc.get());
  addDescription("spaceDesc", folly::toJson(util::toJson(spaceDesc_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> CreateSpaceAsNode::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("oldSpaceName", oldSpaceName_, desc.get());
  addDescription("newSpaceName", newSpaceName_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DropSpace::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("spaceName", spaceName_, desc.get());
  addDescription("ifExists", folly::toJson(util::toJson(ifExists_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ClearSpace::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("spaceName", spaceName_, desc.get());
  addDescription("ifExists", folly::toJson(util::toJson(ifExists_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DescSpace::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("spaceName", spaceName_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ShowCreateSpace::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("spaceName", spaceName_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DropSnapshot::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("snapshotName", snapshotName_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ShowParts::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("spaceId", folly::toJson(util::toJson(spaceId_)), desc.get());
  addDescription("partIds", folly::toJson(util::toJson(partIds_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ShowConfigs::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("module", apache::thrift::util::enumNameSafe(module_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> SetConfig::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("module", apache::thrift::util::enumNameSafe(module_), desc.get());
  addDescription("name", name_, desc.get());
  addDescription("value", value_.toString(), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> GetConfig::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("module", apache::thrift::util::enumNameSafe(module_), desc.get());
  addDescription("name", name_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> CreateNode::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("ifNotExist", folly::toJson(util::toJson(ifNotExist_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DropNode::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("ifExist", folly::toJson(util::toJson(ifExist_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> AddHosts::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("hosts", folly::toJson(util::toJson(hosts_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DropHosts::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("hosts", folly::toJson(util::toJson(hosts_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> CreateUser::explain() const {
  auto desc = CreateNode::explain();
  addDescription("username", *username_, desc.get());
  addDescription("password", "******", desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DropUser::explain() const {
  auto desc = DropNode::explain();
  addDescription("username", *username_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> UpdateUser::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  addDescription("password", "******", desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> GrantRole::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  addDescription("spaceName", *spaceName_, desc.get());
  addDescription("role", apache::thrift::util::enumNameSafe(role_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> RevokeRole::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  addDescription("spaceName", *spaceName_, desc.get());
  addDescription("role", apache::thrift::util::enumNameSafe(role_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ChangePassword::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  addDescription("password", "******", desc.get());
  addDescription("newPassword", "******", desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> DescribeUser::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ListUserRoles::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("username", *username_, desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ListRoles::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("space", folly::toJson(util::toJson(space_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> SubmitJob::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("operation", apache::thrift::util::enumNameSafe(op_), desc.get());
  addDescription("command", apache::thrift::util::enumNameSafe(type_), desc.get());
  addDescription("parameters", folly::toJson(util::toJson(params_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ShowQueries::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("isAll", folly::toJson(util::toJson(isAll())), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> KillQuery::explain() const {
  auto desc = SingleDependencyNode::explain();
  addDescription("sessionId", sessionId()->toString(), desc.get());
  addDescription("planId", epId()->toString(), desc.get());
  return desc;
}
}  // namespace graph
}  // namespace nebula
