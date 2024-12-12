/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_ADMIN_H_
#define GRAPH_PLANNER_PLAN_ADMIN_H_

#include "clients/meta/MetaClient.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/meta_types.h"

// All admin-related nodes would be put in this file.
// These nodes would not exist in a same plan with maintain-related/
// mutate-related/query-related nodes. And they are also isolated
// from each other. This would be guaranteed by parser and validator.
namespace nebula {
namespace graph {

// Some template node such as Create template for the node create
// something(user,tag...) Fit the conflict create process
class CreateNode : public SingleDependencyNode {
 protected:
  CreateNode(QueryContext* qctx, Kind kind, PlanNode* input, bool ifNotExist)
      : SingleDependencyNode(qctx, kind, input), ifNotExist_(ifNotExist) {}

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  bool ifNotExist() const {
    return ifNotExist_;
  }

 private:
  bool ifNotExist_{false};
};

class DropNode : public SingleDependencyNode {
 protected:
  DropNode(QueryContext* qctx, Kind kind, PlanNode* input, bool ifExist)
      : SingleDependencyNode(qctx, kind, input), ifExist_(ifExist) {}

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  bool ifExist() const {
    return ifExist_;
  }

 private:
  bool ifExist_{false};
};

class AddHosts final : public SingleDependencyNode {
 public:
  static AddHosts* make(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts) {
    return qctx->objPool()->makeAndAdd<AddHosts>(qctx, dep, hosts);
  }

  std::vector<HostAddr> getHosts() const {
    return hosts_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  AddHosts(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts)
      : SingleDependencyNode(qctx, Kind::kAddHosts, dep), hosts_(hosts) {}

  std::vector<HostAddr> hosts_;
};

class DropHosts final : public SingleDependencyNode {
 public:
  static DropHosts* make(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts) {
    return qctx->objPool()->makeAndAdd<DropHosts>(qctx, dep, hosts);
  }

  std::vector<HostAddr> getHosts() const {
    return hosts_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  DropHosts(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts)
      : SingleDependencyNode(qctx, Kind::kDropHosts, dep), hosts_(hosts) {}

  std::vector<HostAddr> hosts_;
};

class ShowHosts final : public SingleDependencyNode {
  // TODO(shylock) meta/storage/graph/agent enumerate
 public:
  static ShowHosts* make(QueryContext* qctx, PlanNode* dep, meta::cpp2::ListHostType type) {
    return qctx->objPool()->makeAndAdd<ShowHosts>(qctx, dep, type);
  }

  meta::cpp2::ListHostType getType() const {
    return type_;
  }

 private:
  friend ObjectPool;
  ShowHosts(QueryContext* qctx, PlanNode* dep, meta::cpp2::ListHostType type)
      : SingleDependencyNode(qctx, Kind::kShowHosts, dep), type_(type) {}
  meta::cpp2::ListHostType type_;
};

class ShowMetaLeaderNode final : public SingleDependencyNode {
 public:
  static ShowMetaLeaderNode* make(QueryContext* qctx, PlanNode* dep) {
    return qctx->objPool()->makeAndAdd<ShowMetaLeaderNode>(qctx, dep);
  }

 private:
  friend ObjectPool;
  ShowMetaLeaderNode(QueryContext* qctx, PlanNode* dep)
      : SingleDependencyNode(qctx, Kind::kShowMetaLeader, dep) {}
};

class CreateSpace final : public SingleDependencyNode {
 public:
  static CreateSpace* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::SpaceDesc spaceDesc,
                           bool ifNotExists) {
    return qctx->objPool()->makeAndAdd<CreateSpace>(qctx, input, std::move(spaceDesc), ifNotExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  const meta::cpp2::SpaceDesc& getSpaceDesc() const {
    return spaceDesc_;
  }

  bool getIfNotExists() const {
    return ifNotExists_;
  }

 private:
  friend ObjectPool;
  CreateSpace(QueryContext* qctx,
              PlanNode* input,
              meta::cpp2::SpaceDesc spaceDesc,
              bool ifNotExists)
      : SingleDependencyNode(qctx, Kind::kCreateSpace, input) {
    spaceDesc_ = std::move(spaceDesc);
    ifNotExists_ = ifNotExists;
  }

 private:
  meta::cpp2::SpaceDesc spaceDesc_;
  bool ifNotExists_{false};
};

class CreateSpaceAsNode final : public SingleDependencyNode {
 public:
  static CreateSpaceAsNode* make(QueryContext* qctx,
                                 PlanNode* input,
                                 const std::string& oldSpaceName,
                                 const std::string& newSpaceName,
                                 bool ifNotExists) {
    return qctx->objPool()->makeAndAdd<CreateSpaceAsNode>(
        qctx, input, oldSpaceName, newSpaceName, ifNotExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  std::string getOldSpaceName() const {
    return oldSpaceName_;
  }

  std::string getNewSpaceName() const {
    return newSpaceName_;
  }

  bool getIfNotExists() const {
    return ifNotExists_;
  }

 private:
  friend ObjectPool;
  CreateSpaceAsNode(QueryContext* qctx,
                    PlanNode* input,
                    std::string oldName,
                    std::string newName,
                    bool ifNotExists)
      : SingleDependencyNode(qctx, Kind::kCreateSpaceAs, input),
        oldSpaceName_(std::move(oldName)),
        newSpaceName_(std::move(newName)),
        ifNotExists_(ifNotExists) {}

 private:
  std::string oldSpaceName_;
  std::string newSpaceName_;
  bool ifNotExists_{false};
};

class DropSpace final : public SingleDependencyNode {
 public:
  static DropSpace* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string spaceName,
                         bool ifExists) {
    return qctx->objPool()->makeAndAdd<DropSpace>(qctx, input, std::move(spaceName), ifExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

  bool getIfExists() const {
    return ifExists_;
  }

 private:
  friend ObjectPool;
  DropSpace(QueryContext* qctx, PlanNode* input, std::string spaceName, bool ifExists)
      : SingleDependencyNode(qctx, Kind::kDropSpace, input) {
    spaceName_ = std::move(spaceName);
    ifExists_ = ifExists;
  }

 private:
  std::string spaceName_;
  bool ifExists_;
};

class ClearSpace final : public SingleDependencyNode {
 public:
  static ClearSpace* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string spaceName,
                          bool ifExists) {
    return qctx->objPool()->makeAndAdd<ClearSpace>(qctx, input, std::move(spaceName), ifExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

  bool getIfExists() const {
    return ifExists_;
  }

 private:
  friend ObjectPool;
  ClearSpace(QueryContext* qctx, PlanNode* input, std::string spaceName, bool ifExists)
      : SingleDependencyNode(qctx, Kind::kClearSpace, input) {
    spaceName_ = std::move(spaceName);
    ifExists_ = ifExists;
  }

 private:
  std::string spaceName_;
  bool ifExists_;
};

class AlterSpace final : public SingleDependencyNode {
 public:
  static AlterSpace* make(QueryContext* qctx,
                          PlanNode* input,
                          const std::string& spaceName,
                          meta::cpp2::AlterSpaceOp op,
                          const std::vector<std::string>& paras) {
    return qctx->objPool()->makeAndAdd<AlterSpace>(qctx, input, spaceName, op, paras);
  }
  const std::string& getSpaceName() const {
    return spaceName_;
  }

  meta::cpp2::AlterSpaceOp getAlterSpaceOp() const {
    return op_;
  }

  const std::vector<std::string>& getParas() const {
    return paras_;
  }

 private:
  friend ObjectPool;
  AlterSpace(QueryContext* qctx,
             PlanNode* input,
             const std::string& spaceName,
             meta::cpp2::AlterSpaceOp op,
             const std::vector<std::string>& paras)
      : SingleDependencyNode(qctx, Kind::kAlterSpace, input),
        spaceName_(spaceName),
        op_(op),
        paras_(paras) {}

 private:
  std::string spaceName_;
  meta::cpp2::AlterSpaceOp op_;
  std::vector<std::string> paras_;
};

class DescSpace final : public SingleDependencyNode {
 public:
  static DescSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
    return qctx->objPool()->makeAndAdd<DescSpace>(qctx, input, std::move(spaceName));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

 private:
  friend ObjectPool;
  DescSpace(QueryContext* qctx, PlanNode* input, std::string spaceName)
      : SingleDependencyNode(qctx, Kind::kDescSpace, input) {
    spaceName_ = std::move(spaceName);
  }

 private:
  std::string spaceName_;
};

class ShowSpaces final : public SingleDependencyNode {
 public:
  static ShowSpaces* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowSpaces>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowSpaces(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowSpaces, input) {}
};

class ShowConfigs final : public SingleDependencyNode {
 public:
  static ShowConfigs* make(QueryContext* qctx, PlanNode* input, meta::cpp2::ConfigModule module) {
    return qctx->objPool()->makeAndAdd<ShowConfigs>(qctx, input, module);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  meta::cpp2::ConfigModule getModule() const {
    return module_;
  }

 private:
  friend ObjectPool;
  ShowConfigs(QueryContext* qctx, PlanNode* input, meta::cpp2::ConfigModule module)
      : SingleDependencyNode(qctx, Kind::kShowConfigs, input), module_(module) {}

 private:
  meta::cpp2::ConfigModule module_;
};

class SetConfig final : public SingleDependencyNode {
 public:
  static SetConfig* make(QueryContext* qctx,
                         PlanNode* input,
                         meta::cpp2::ConfigModule module,
                         std::string name,
                         Value value) {
    return qctx->objPool()->makeAndAdd<SetConfig>(
        qctx, input, module, std::move(name), std::move(value));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  meta::cpp2::ConfigModule getModule() const {
    return module_;
  }

  const std::string& getName() const {
    return name_;
  }

  const Value& getValue() const {
    return value_;
  }

 private:
  friend ObjectPool;
  SetConfig(QueryContext* qctx,
            PlanNode* input,
            meta::cpp2::ConfigModule module,
            std::string name,
            Value value)
      : SingleDependencyNode(qctx, Kind::kSetConfig, input),
        module_(module),
        name_(std::move(name)),
        value_(std::move(value)) {}

 private:
  meta::cpp2::ConfigModule module_;
  std::string name_;
  Value value_;
};

class GetConfig final : public SingleDependencyNode {
 public:
  static GetConfig* make(QueryContext* qctx,
                         PlanNode* input,
                         meta::cpp2::ConfigModule module,
                         std::string name) {
    return qctx->objPool()->makeAndAdd<GetConfig>(qctx, input, module, std::move(name));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  meta::cpp2::ConfigModule getModule() const {
    return module_;
  }

  const std::string& getName() const {
    return name_;
  }

 private:
  friend ObjectPool;
  GetConfig(QueryContext* qctx, PlanNode* input, meta::cpp2::ConfigModule module, std::string name)
      : SingleDependencyNode(qctx, Kind::kGetConfig, input),
        module_(module),
        name_(std::move(name)) {}

 private:
  meta::cpp2::ConfigModule module_;
  std::string name_;
};

class ShowCreateSpace final : public SingleDependencyNode {
 public:
  static ShowCreateSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
    return qctx->objPool()->makeAndAdd<ShowCreateSpace>(qctx, input, std::move(spaceName));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

 private:
  friend ObjectPool;
  ShowCreateSpace(QueryContext* qctx, PlanNode* input, std::string spaceName)
      : SingleDependencyNode(qctx, Kind::kShowCreateSpace, input) {
    spaceName_ = std::move(spaceName);
  }

 private:
  std::string spaceName_;
};

class CreateSnapshot final : public SingleDependencyNode {
 public:
  static CreateSnapshot* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<CreateSnapshot>(qctx, input);
  }

 private:
  friend ObjectPool;
  CreateSnapshot(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kCreateSnapshot, input) {}
};

class DropSnapshot final : public SingleDependencyNode {
 public:
  static DropSnapshot* make(QueryContext* qctx, PlanNode* input, std::string snapshotName) {
    return qctx->objPool()->makeAndAdd<DropSnapshot>(qctx, input, std::move(snapshotName));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSnapshotName() const {
    return snapshotName_;
  }

 private:
  friend ObjectPool;
  DropSnapshot(QueryContext* qctx, PlanNode* input, std::string snapshotName)
      : SingleDependencyNode(qctx, Kind::kDropSnapshot, input) {
    snapshotName_ = std::move(snapshotName);
  }

 private:
  std::string snapshotName_;
};

class ShowSnapshots final : public SingleDependencyNode {
 public:
  static ShowSnapshots* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowSnapshots>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowSnapshots(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowSnapshots, input) {}
};

class AddListener final : public SingleDependencyNode {
 public:
  static AddListener* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::ListenerType type,
                           std::vector<HostAddr> hosts) {
    return qctx->objPool()->makeAndAdd<AddListener>(qctx, input, std::move(type), std::move(hosts));
  }

  const meta::cpp2::ListenerType& type() const {
    return type_;
  }

  const std::vector<HostAddr> hosts() const {
    return hosts_;
  }

 private:
  friend ObjectPool;
  AddListener(QueryContext* qctx,
              PlanNode* input,
              meta::cpp2::ListenerType type,
              std::vector<HostAddr> hosts)
      : SingleDependencyNode(qctx, Kind::kAddListener, input) {
    type_ = std::move(type);
    hosts_ = std::move(hosts);
  }

 private:
  meta::cpp2::ListenerType type_;
  std::vector<HostAddr> hosts_;
};

class RemoveListener final : public SingleDependencyNode {
 public:
  static RemoveListener* make(QueryContext* qctx, PlanNode* input, meta::cpp2::ListenerType type) {
    return qctx->objPool()->makeAndAdd<RemoveListener>(qctx, input, std::move(type));
  }

  const meta::cpp2::ListenerType& type() const {
    return type_;
  }

 private:
  friend ObjectPool;
  RemoveListener(QueryContext* qctx, PlanNode* input, meta::cpp2::ListenerType type)
      : SingleDependencyNode(qctx, Kind::kRemoveListener, input) {
    type_ = std::move(type);
  }

 private:
  meta::cpp2::ListenerType type_;
};

class ShowListener final : public SingleDependencyNode {
 public:
  static ShowListener* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowListener>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowListener(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowListener, input) {}
};

// User related Node
class CreateUser final : public CreateNode {
 public:
  static CreateUser* make(QueryContext* qctx,
                          PlanNode* dep,
                          const std::string* username,
                          const std::string* password,
                          bool ifNotExists) {
    return qctx->objPool()->makeAndAdd<CreateUser>(qctx, dep, username, password, ifNotExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* password() const {
    return password_;
  }

 private:
  friend ObjectPool;
  CreateUser(QueryContext* qctx,
             PlanNode* dep,
             const std::string* username,
             const std::string* password,
             bool ifNotExists)
      : CreateNode(qctx, Kind::kCreateUser, dep, ifNotExists),
        username_(username),
        password_(password) {}

 private:
  const std::string* username_;
  const std::string* password_;
};

class DropUser final : public DropNode {
 public:
  static DropUser* make(QueryContext* qctx,
                        PlanNode* dep,
                        const std::string* username,
                        bool ifNotExists) {
    return qctx->objPool()->makeAndAdd<DropUser>(qctx, dep, username, ifNotExists);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
  friend ObjectPool;
  DropUser(QueryContext* qctx, PlanNode* dep, const std::string* username, bool ifNotExists)
      : DropNode(qctx, Kind::kDropUser, dep, ifNotExists), username_(username) {}

 private:
  const std::string* username_;
};

class UpdateUser final : public SingleDependencyNode {
 public:
  static UpdateUser* make(QueryContext* qctx,
                          PlanNode* dep,
                          const std::string* username,
                          const std::string* password) {
    return qctx->objPool()->makeAndAdd<UpdateUser>(qctx, dep, username, password);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* password() const {
    return password_;
  }

 private:
  friend ObjectPool;
  UpdateUser(QueryContext* qctx,
             PlanNode* dep,
             const std::string* username,
             const std::string* password)
      : SingleDependencyNode(qctx, Kind::kUpdateUser, dep),
        username_(username),
        password_(password) {}

 private:
  const std::string* username_;
  const std::string* password_;
};

class GrantRole final : public SingleDependencyNode {
 public:
  static GrantRole* make(QueryContext* qctx,
                         PlanNode* dep,
                         const std::string* username,
                         const std::string* spaceName,
                         meta::cpp2::RoleType role) {
    return qctx->objPool()->makeAndAdd<GrantRole>(qctx, dep, username, spaceName, role);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* spaceName() const {
    return spaceName_;
  }

  meta::cpp2::RoleType role() const {
    return role_;
  }

 private:
  friend ObjectPool;
  GrantRole(QueryContext* qctx,
            PlanNode* dep,
            const std::string* username,
            const std::string* spaceName,
            meta::cpp2::RoleType role)
      : SingleDependencyNode(qctx, Kind::kGrantRole, dep),
        username_(username),
        spaceName_(spaceName),
        role_(role) {}

 private:
  const std::string* username_;
  const std::string* spaceName_;
  meta::cpp2::RoleType role_;
};

class RevokeRole final : public SingleDependencyNode {
 public:
  static RevokeRole* make(QueryContext* qctx,
                          PlanNode* dep,
                          const std::string* username,
                          const std::string* spaceName,
                          meta::cpp2::RoleType role) {
    return qctx->objPool()->makeAndAdd<RevokeRole>(qctx, dep, username, spaceName, role);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* spaceName() const {
    return spaceName_;
  }

  meta::cpp2::RoleType role() const {
    return role_;
  }

 private:
  friend ObjectPool;
  RevokeRole(QueryContext* qctx,
             PlanNode* dep,
             const std::string* username,
             const std::string* spaceName,
             meta::cpp2::RoleType role)
      : SingleDependencyNode(qctx, Kind::kRevokeRole, dep),
        username_(username),
        spaceName_(spaceName),
        role_(role) {}

 private:
  const std::string* username_;
  const std::string* spaceName_;
  meta::cpp2::RoleType role_;
};

class ChangePassword final : public SingleDependencyNode {
 public:
  static ChangePassword* make(QueryContext* qctx,
                              PlanNode* dep,
                              const std::string* username,
                              const std::string* password,
                              const std::string* newPassword) {
    return qctx->objPool()->makeAndAdd<ChangePassword>(qctx, dep, username, password, newPassword);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* password() const {
    return password_;
  }

  const std::string* newPassword() const {
    return newPassword_;
  }

 private:
  friend ObjectPool;
  ChangePassword(QueryContext* qctx,
                 PlanNode* dep,
                 const std::string* username,
                 const std::string* password,
                 const std::string* newPassword)
      : SingleDependencyNode(qctx, Kind::kChangePassword, dep),
        username_(username),
        password_(password),
        newPassword_(newPassword) {}

 private:
  const std::string* username_;
  const std::string* password_;
  const std::string* newPassword_;
};

class ListUserRoles final : public SingleDependencyNode {
 public:
  static ListUserRoles* make(QueryContext* qctx, PlanNode* dep, const std::string* username) {
    return qctx->objPool()->makeAndAdd<ListUserRoles>(qctx, dep, username);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
  friend ObjectPool;
  ListUserRoles(QueryContext* qctx, PlanNode* dep, const std::string* username)
      : SingleDependencyNode(qctx, Kind::kListUserRoles, dep), username_(username) {}

 private:
  const std::string* username_;
};

class ListUsers final : public SingleDependencyNode {
 public:
  static ListUsers* make(QueryContext* qctx, PlanNode* dep) {
    return qctx->objPool()->makeAndAdd<ListUsers>(qctx, dep);
  }

 private:
  friend ObjectPool;
  ListUsers(QueryContext* qctx, PlanNode* dep)
      : SingleDependencyNode(qctx, Kind::kListUsers, dep) {}
};

class DescribeUser final : public SingleDependencyNode {
 public:
  static DescribeUser* make(QueryContext* qctx, PlanNode* dep, const std::string* username) {
    return qctx->objPool()->makeAndAdd<DescribeUser>(qctx, dep, username);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
  friend ObjectPool;
  DescribeUser(QueryContext* qctx, PlanNode* dep, const std::string* username)
      : SingleDependencyNode(qctx, Kind::kDescribeUser, dep), username_(username) {}

  const std::string* username_;
};

class ListRoles final : public SingleDependencyNode {
 public:
  static ListRoles* make(QueryContext* qctx, PlanNode* dep, GraphSpaceID space) {
    return qctx->objPool()->makeAndAdd<ListRoles>(qctx, dep, space);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  GraphSpaceID space() const {
    return space_;
  }

 private:
  friend ObjectPool;
  ListRoles(QueryContext* qctx, PlanNode* dep, GraphSpaceID space)
      : SingleDependencyNode(qctx, Kind::kListRoles, dep), space_(space) {}

  GraphSpaceID space_{-1};
};

class ShowParts final : public SingleDependencyNode {
 public:
  static ShowParts* make(QueryContext* qctx,
                         PlanNode* input,
                         GraphSpaceID spaceId,
                         std::vector<PartitionID> partIds) {
    return qctx->objPool()->makeAndAdd<ShowParts>(qctx, input, spaceId, std::move(partIds));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  GraphSpaceID getSpaceId() const {
    return spaceId_;
  }

  const std::vector<PartitionID>& getPartIds() const {
    return partIds_;
  }

 private:
  ShowParts(QueryContext* qctx,
            PlanNode* input,
            GraphSpaceID spaceId,
            std::vector<PartitionID> partIds)
      : SingleDependencyNode(qctx, Kind::kShowParts, input) {
    spaceId_ = spaceId;
    partIds_ = std::move(partIds);
  }

 private:
  friend ObjectPool;
  GraphSpaceID spaceId_{-1};
  std::vector<PartitionID> partIds_;
};

class SubmitJob final : public SingleDependencyNode {
 public:
  static SubmitJob* make(QueryContext* qctx,
                         PlanNode* dep,
                         meta::cpp2::JobOp op,
                         meta::cpp2::JobType type,
                         const std::vector<std::string>& params) {
    return qctx->objPool()->makeAndAdd<SubmitJob>(qctx, dep, op, type, params);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  meta::cpp2::JobOp jobOp() const {
    return op_;
  }

  meta::cpp2::JobType jobType() const {
    return type_;
  }

  const std::vector<std::string>& params() const {
    return params_;
  }

 private:
  friend ObjectPool;
  SubmitJob(QueryContext* qctx,
            PlanNode* dep,
            meta::cpp2::JobOp op,
            meta::cpp2::JobType type,
            const std::vector<std::string>& params)
      : SingleDependencyNode(qctx, Kind::kSubmitJob, dep), op_(op), type_(type), params_(params) {}

 private:
  meta::cpp2::JobOp op_;
  meta::cpp2::JobType type_;
  const std::vector<std::string> params_;
};

class TransferLeader final : public SingleDependencyNode {
 public:
  static TransferLeader* make(QueryContext* qctx,
                              PlanNode* input,
                              HostAddr address,
                              std::string spaceName,
                              int32_t concurrency) {
    return qctx->objPool()->add(
        new TransferLeader(qctx, input, std::move(address), std::move(spaceName), concurrency));
  }

  const std::string& spaceName() const { return spaceName_; }

  const HostAddr& address() const { return address_; }

  int32_t concurrency() const { return concurrency_; }

 private:
  TransferLeader(QueryContext* qctx,
                 PlanNode* input,
                 HostAddr address,
                 std::string spaceName,
                 int32_t concurrency)
      : SingleDependencyNode(qctx, Kind::kTransferLeader, input) {
    spaceName_ = std::move(spaceName);
    address_ = std::move(address);
    concurrency_ = concurrency;
  }

 private:
  std::string spaceName_;
  HostAddr address_;
  int32_t concurrency_;
};


class ShowCharset final : public SingleDependencyNode {
 public:
  static ShowCharset* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowCharset>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowCharset(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowCharset, input) {}
};

class ShowCollation final : public SingleDependencyNode {
 public:
  static ShowCollation* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowCollation>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowCollation(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowCollation, input) {}
};

class AddHostsIntoZone final : public SingleDependencyNode {
 public:
  static AddHostsIntoZone* make(QueryContext* qctx,
                                PlanNode* input,
                                std::string zoneName,
                                std::vector<HostAddr> addresses,
                                bool isNew) {
    return qctx->objPool()->makeAndAdd<AddHostsIntoZone>(
        qctx, input, std::move(zoneName), std::move(addresses), isNew);
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

  const std::vector<HostAddr>& address() const {
    return addresses_;
  }

  bool isNew() const {
    return isNew_;
  }

 private:
  friend ObjectPool;
  AddHostsIntoZone(QueryContext* qctx,
                   PlanNode* input,
                   std::string zoneName,
                   std::vector<HostAddr> addresses,
                   bool isNew)
      : SingleDependencyNode(qctx, Kind::kAddHostsIntoZone, input) {
    zoneName_ = std::move(zoneName);
    addresses_ = std::move(addresses);
    isNew_ = isNew;
  }

 private:
  std::string zoneName_;
  std::vector<HostAddr> addresses_;
  bool isNew_;
};

class MergeZone final : public SingleDependencyNode {
 public:
  static MergeZone* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string zoneName,
                         std::vector<std::string> zoneNames) {
    return qctx->objPool()->makeAndAdd<MergeZone>(
        qctx, input, std::move(zoneName), std::move(zoneNames));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

  const std::vector<std::string>& zones() const {
    return zones_;
  }

 private:
  friend ObjectPool;
  MergeZone(QueryContext* qctx,
            PlanNode* input,
            std::string zoneName,
            std::vector<std::string> zoneNames)
      : SingleDependencyNode(qctx, Kind::kMergeZone, input) {
    zoneName_ = std::move(zoneName);
    zones_ = std::move(zoneNames);
  }

 private:
  std::string zoneName_;
  std::vector<std::string> zones_;
};

class RenameZone final : public SingleDependencyNode {
 public:
  static RenameZone* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string originalZoneName,
                          std::string zoneName) {
    return qctx->objPool()->makeAndAdd<RenameZone>(
        qctx, input, std::move(originalZoneName), std::move(zoneName));
  }

  const std::string& originalZoneName() const {
    return originalZoneName_;
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
  friend ObjectPool;
  RenameZone(QueryContext* qctx,
             PlanNode* input,
             std::string originalZoneName,
             std::string zoneName)
      : SingleDependencyNode(qctx, Kind::kRenameZone, input) {
    originalZoneName_ = std::move(originalZoneName);
    zoneName_ = std::move(zoneName);
  }

 private:
  std::string originalZoneName_;
  std::string zoneName_;
};

class DropZone final : public SingleDependencyNode {
 public:
  static DropZone* make(QueryContext* qctx, PlanNode* input, std::string zoneName) {
    return qctx->objPool()->makeAndAdd<DropZone>(qctx, input, std::move(zoneName));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
  friend ObjectPool;
  DropZone(QueryContext* qctx, PlanNode* input, std::string zoneName)
      : SingleDependencyNode(qctx, Kind::kDropZone, input) {
    zoneName_ = std::move(zoneName);
  }

 private:
  std::string zoneName_;
};

class DivideZone final : public SingleDependencyNode {
 public:
  static DivideZone* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string zoneName,
                          std::unordered_map<std::string, std::vector<HostAddr>> zoneItems) {
    return qctx->objPool()->makeAndAdd<DivideZone>(
        qctx, input, std::move(zoneName), std::move(zoneItems));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

  const std::unordered_map<std::string, std::vector<HostAddr>>& zoneItems() const {
    return zoneItems_;
  }

 private:
  friend ObjectPool;
  DivideZone(QueryContext* qctx,
             PlanNode* input,
             std::string zoneName,
             std::unordered_map<std::string, std::vector<HostAddr>> zoneItems)
      : SingleDependencyNode(qctx, Kind::kDivideZone, input) {
    zoneName_ = std::move(zoneName);
    zoneItems_ = std::move(zoneItems);
  }

 private:
  std::string zoneName_;
  std::unordered_map<std::string, std::vector<HostAddr>> zoneItems_;
};

class DescribeZone final : public SingleDependencyNode {
 public:
  static DescribeZone* make(QueryContext* qctx, PlanNode* input, std::string zoneName) {
    return qctx->objPool()->makeAndAdd<DescribeZone>(qctx, input, std::move(zoneName));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
  friend ObjectPool;
  DescribeZone(QueryContext* qctx, PlanNode* input, std::string zoneName)
      : SingleDependencyNode(qctx, Kind::kDescribeZone, input) {
    zoneName_ = std::move(zoneName);
  }

 private:
  std::string zoneName_;
};

class ListZones final : public SingleDependencyNode {
 public:
  static ListZones* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ListZones>(qctx, input);
  }

 private:
  friend ObjectPool;
  ListZones(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowZones, input) {}
};

class ShowZones final : public SingleDependencyNode {
 public:
  static ShowZones* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowZones>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowZones(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowZones, input) {}
};

class ShowStats final : public SingleDependencyNode {
 public:
  static ShowStats* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<ShowStats>(qctx, input);
  }

 private:
  friend ObjectPool;
  ShowStats(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowStats, input) {}
};

class ShowServiceClients final : public SingleDependencyNode {
 public:
  static ShowServiceClients* make(QueryContext* qctx,
                                  PlanNode* input,
                                  meta::cpp2::ExternalServiceType type) {
    return qctx->objPool()->makeAndAdd<ShowServiceClients>(qctx, input, type);
  }

  meta::cpp2::ExternalServiceType type() const {
    return type_;
  }

 private:
  friend ObjectPool;
  ShowServiceClients(QueryContext* qctx, PlanNode* input, meta::cpp2::ExternalServiceType type)
      : SingleDependencyNode(qctx, Kind::kShowServiceClients, input), type_(type) {}

 private:
  meta::cpp2::ExternalServiceType type_;
};

class SignInService final : public SingleDependencyNode {
 public:
  static SignInService* make(QueryContext* qctx,
                             PlanNode* input,
                             std::vector<meta::cpp2::ServiceClient> clients,
                             meta::cpp2::ExternalServiceType type) {
    return qctx->objPool()->makeAndAdd<SignInService>(qctx, input, std::move(clients), type);
  }

  const std::vector<meta::cpp2::ServiceClient>& clients() const {
    return clients_;
  }

  meta::cpp2::ExternalServiceType type() const {
    return type_;
  }

 private:
  friend ObjectPool;
  SignInService(QueryContext* qctx,
                PlanNode* input,
                std::vector<meta::cpp2::ServiceClient> clients,
                meta::cpp2::ExternalServiceType type)
      : SingleDependencyNode(qctx, Kind::kSignInService, input),
        clients_(std::move(clients)),
        type_(type) {}

 private:
  std::vector<meta::cpp2::ServiceClient> clients_;
  meta::cpp2::ExternalServiceType type_;
};

class SignOutService final : public SingleDependencyNode {
 public:
  static SignOutService* make(QueryContext* qctx,
                              PlanNode* input,
                              meta::cpp2::ExternalServiceType type) {
    return qctx->objPool()->makeAndAdd<SignOutService>(qctx, input, type);
  }

  meta::cpp2::ExternalServiceType type() const {
    return type_;
  }

 private:
  friend ObjectPool;
  SignOutService(QueryContext* qctx, PlanNode* input, meta::cpp2::ExternalServiceType type)
      : SingleDependencyNode(qctx, Kind::kSignOutService, input), type_(type) {}

 private:
  meta::cpp2::ExternalServiceType type_;
};

class ShowSessions final : public SingleInputNode {
 public:
  static ShowSessions* make(QueryContext* qctx,
                            PlanNode* input,
                            bool isSetSessionID,
                            SessionID sessionId,
                            bool isLocalCommand) {
    return qctx->objPool()->makeAndAdd<ShowSessions>(
        qctx, input, isSetSessionID, sessionId, isLocalCommand);
  }

  bool isSetSessionID() const {
    return isSetSessionID_;
  }
  bool isLocalCommand() const {
    return isLocalCommand_;
  }
  SessionID getSessionId() const {
    return sessionId_;
  }

 private:
  friend ObjectPool;
  ShowSessions(QueryContext* qctx,
               PlanNode* input,
               bool isSetSessionID,
               SessionID sessionId,
               bool isLocalCommand)
      : SingleInputNode(qctx, Kind::kShowSessions, input) {
    sessionId_ = sessionId;
    isSetSessionID_ = isSetSessionID;
    isLocalCommand_ = isLocalCommand;
  }

 private:
  SessionID sessionId_{-1};
  bool isSetSessionID_{false};
  bool isLocalCommand_{false};
};

class KillSession final : public SingleInputNode {
 public:
  static KillSession* make(QueryContext* qctx, PlanNode* input, Expression* sessionId) {
    return qctx->objPool()->makeAndAdd<KillSession>(qctx, input, sessionId);
  }

  Expression* getSessionId() const {
    return sessionId_;
  }

 private:
  friend ObjectPool;
  KillSession(QueryContext* qctx, PlanNode* input, Expression* sessionId)
      : SingleInputNode(qctx, Kind::kKillSession, input), sessionId_(sessionId) {}

 private:
  Expression* sessionId_{nullptr};
};

class UpdateSession final : public SingleInputNode {
 public:
  static UpdateSession* make(QueryContext* qctx, PlanNode* input, meta::cpp2::Session session) {
    return qctx->objPool()->makeAndAdd<UpdateSession>(qctx, input, std::move(session));
  }

  const meta::cpp2::Session& getSession() const {
    return session_;
  }

 private:
  friend ObjectPool;
  UpdateSession(QueryContext* qctx, PlanNode* input, meta::cpp2::Session session)
      : SingleInputNode(qctx, Kind::kUpdateSession, input), session_(std::move(session)) {}

 private:
  meta::cpp2::Session session_;
};

class ShowQueries final : public SingleInputNode {
 public:
  static ShowQueries* make(QueryContext* qctx, PlanNode* input, bool isAll) {
    return qctx->objPool()->makeAndAdd<ShowQueries>(qctx, input, isAll);
  }

  bool isAll() const {
    return isAll_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  ShowQueries(QueryContext* qctx, PlanNode* input, bool isAll)
      : SingleInputNode(qctx, Kind::kShowQueries, input), isAll_(isAll) {}

  bool isAll_{false};
};

class KillQuery final : public SingleInputNode {
 public:
  static KillQuery* make(QueryContext* qctx,
                         PlanNode* input,
                         Expression* sessionId,
                         Expression* epId) {
    return qctx->objPool()->makeAndAdd<KillQuery>(qctx, input, sessionId, epId);
  }

  Expression* sessionId() const {
    return sessionId_;
  }

  Expression* epId() const {
    return epId_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  KillQuery(QueryContext* qctx, PlanNode* input, Expression* sessionId, Expression* epId)
      : SingleInputNode(qctx, Kind::kKillQuery, input), sessionId_(sessionId), epId_(epId) {}

  Expression* sessionId_;
  Expression* epId_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLAN_ADMIN_H_
