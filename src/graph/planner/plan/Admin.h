/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_ADMIN_H_
#define GRAPH_PLANNER_PLAN_ADMIN_H_

#include "clients/meta/MetaClient.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/meta_types.h"

/**
 * All admin-related nodes would be put in this file.
 * These nodes would not exist in a same plan with maintain-related/
 * mutate-related/query-related nodes. And they are also isolated
 * from each other. This would be guaranteed by parser and validator.
 */
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
    return qctx->objPool()->add(new AddHosts(qctx, dep, hosts));
  }

  std::vector<HostAddr> getHosts() const {
    return hosts_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  AddHosts(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts)
      : SingleDependencyNode(qctx, Kind::kAddHosts, dep), hosts_(hosts) {}

  std::vector<HostAddr> hosts_;
};

class DropHosts final : public SingleDependencyNode {
 public:
  static DropHosts* make(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts) {
    return qctx->objPool()->add(new DropHosts(qctx, dep, hosts));
  }

  std::vector<HostAddr> getHosts() const {
    return hosts_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  DropHosts(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> hosts)
      : SingleDependencyNode(qctx, Kind::kDropHosts, dep), hosts_(hosts) {}

  std::vector<HostAddr> hosts_;
};

class ShowHosts final : public SingleDependencyNode {
  // TODO(shylock) meta/storage/graph enumerate
 public:
  static ShowHosts* make(QueryContext* qctx, PlanNode* dep, meta::cpp2::ListHostType type) {
    return qctx->objPool()->add(new ShowHosts(qctx, dep, type));
  }

  meta::cpp2::ListHostType getType() const {
    return type_;
  }

 private:
  ShowHosts(QueryContext* qctx, PlanNode* dep, meta::cpp2::ListHostType type)
      : SingleDependencyNode(qctx, Kind::kShowHosts, dep), type_(type) {}
  meta::cpp2::ListHostType type_;
};

class ShowMetaLeaderNode final : public SingleDependencyNode {
 public:
  static ShowMetaLeaderNode* make(QueryContext* qctx, PlanNode* dep) {
    return qctx->objPool()->add(new ShowMetaLeaderNode(qctx, dep));
  }

 private:
  ShowMetaLeaderNode(QueryContext* qctx, PlanNode* dep)
      : SingleDependencyNode(qctx, Kind::kShowMetaLeader, dep) {}
};

class CreateSpace final : public SingleDependencyNode {
 public:
  static CreateSpace* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::SpaceDesc spaceDesc,
                           bool ifNotExists) {
    return qctx->objPool()->add(new CreateSpace(qctx, input, std::move(spaceDesc), ifNotExists));
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
                                 const std::string& newSpaceName) {
    return qctx->objPool()->add(new CreateSpaceAsNode(qctx, input, oldSpaceName, newSpaceName));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  std::string getOldSpaceName() const {
    return oldSpaceName_;
  }

  std::string getNewSpaceName() const {
    return newSpaceName_;
  }

 private:
  CreateSpaceAsNode(QueryContext* qctx, PlanNode* input, std::string oldName, std::string newName)
      : SingleDependencyNode(qctx, Kind::kCreateSpaceAs, input),
        oldSpaceName_(std::move(oldName)),
        newSpaceName_(std::move(newName)) {}

 private:
  std::string oldSpaceName_;
  std::string newSpaceName_;
};

class DropSpace final : public SingleDependencyNode {
 public:
  static DropSpace* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string spaceName,
                         bool ifExists) {
    return qctx->objPool()->add(new DropSpace(qctx, input, std::move(spaceName), ifExists));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

  bool getIfExists() const {
    return ifExists_;
  }

 private:
  DropSpace(QueryContext* qctx, PlanNode* input, std::string spaceName, bool ifExists)
      : SingleDependencyNode(qctx, Kind::kDropSpace, input) {
    spaceName_ = std::move(spaceName);
    ifExists_ = ifExists;
  }

 private:
  std::string spaceName_;
  bool ifExists_;
};

class DescSpace final : public SingleDependencyNode {
 public:
  static DescSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
    return qctx->objPool()->add(new DescSpace(qctx, input, std::move(spaceName)));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

 private:
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
    return qctx->objPool()->add(new ShowSpaces(qctx, input));
  }

 private:
  explicit ShowSpaces(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowSpaces, input) {}
};

class ShowConfigs final : public SingleDependencyNode {
 public:
  static ShowConfigs* make(QueryContext* qctx, PlanNode* input, meta::cpp2::ConfigModule module) {
    return qctx->objPool()->add(new ShowConfigs(qctx, input, module));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  meta::cpp2::ConfigModule getModule() const {
    return module_;
  }

 private:
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
    return qctx->objPool()->add(
        new SetConfig(qctx, input, module, std::move(name), std::move(value)));
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
    return qctx->objPool()->add(new GetConfig(qctx, input, module, std::move(name)));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  meta::cpp2::ConfigModule getModule() const {
    return module_;
  }

  const std::string& getName() const {
    return name_;
  }

 private:
  explicit GetConfig(QueryContext* qctx,
                     PlanNode* input,
                     meta::cpp2::ConfigModule module,
                     std::string name)
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
    return qctx->objPool()->add(new ShowCreateSpace(qctx, input, std::move(spaceName)));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSpaceName() const {
    return spaceName_;
  }

 private:
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
    return qctx->objPool()->add(new CreateSnapshot(qctx, input));
  }

 private:
  explicit CreateSnapshot(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kCreateSnapshot, input) {}
};

class DropSnapshot final : public SingleDependencyNode {
 public:
  static DropSnapshot* make(QueryContext* qctx, PlanNode* input, std::string snapshotName) {
    return qctx->objPool()->add(new DropSnapshot(qctx, input, std::move(snapshotName)));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string& getSnapshotName() const {
    return snapshotName_;
  }

 private:
  explicit DropSnapshot(QueryContext* qctx, PlanNode* input, std::string snapshotName)
      : SingleDependencyNode(qctx, Kind::kDropSnapshot, input) {
    snapshotName_ = std::move(snapshotName);
  }

 private:
  std::string snapshotName_;
};

class ShowSnapshots final : public SingleDependencyNode {
 public:
  static ShowSnapshots* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowSnapshots(qctx, input));
  }

 private:
  explicit ShowSnapshots(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowSnapshots, input) {}
};

class AddListener final : public SingleDependencyNode {
 public:
  static AddListener* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::ListenerType type,
                           std::vector<HostAddr> hosts) {
    return qctx->objPool()->add(new AddListener(qctx, input, std::move(type), std::move(hosts)));
  }

  const meta::cpp2::ListenerType& type() const {
    return type_;
  }

  const std::vector<HostAddr> hosts() const {
    return hosts_;
  }

 private:
  explicit AddListener(QueryContext* qctx,
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
    return qctx->objPool()->add(new RemoveListener(qctx, input, std::move(type)));
  }

  const meta::cpp2::ListenerType& type() const {
    return type_;
  }

 private:
  explicit RemoveListener(QueryContext* qctx, PlanNode* input, meta::cpp2::ListenerType type)
      : SingleDependencyNode(qctx, Kind::kRemoveListener, input) {
    type_ = std::move(type);
  }

 private:
  meta::cpp2::ListenerType type_;
};

class ShowListener final : public SingleDependencyNode {
 public:
  static ShowListener* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowListener(qctx, input));
  }

 private:
  explicit ShowListener(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowListener, input) {}
};

class Download final : public SingleDependencyNode {
 public:
  static Download* make(QueryContext* qctx,
                        PlanNode* input,
                        std::string hdfsHost,
                        int32_t hdfsPort,
                        std::string hdfsPath) {
    return qctx->objPool()->add(new Download(qctx, input, hdfsHost, hdfsPort, hdfsPath));
  }

  const std::string& getHdfsHost() const {
    return hdfsHost_;
  }

  int32_t getHdfsPort() const {
    return hdfsPort_;
  }

  const std::string& getHdfsPath() const {
    return hdfsPath_;
  }

 private:
  explicit Download(QueryContext* qctx,
                    PlanNode* dep,
                    std::string hdfsHost,
                    int32_t hdfsPort,
                    std::string hdfsPath)
      : SingleDependencyNode(qctx, Kind::kDownload, dep),
        hdfsHost_(hdfsHost),
        hdfsPort_(hdfsPort),
        hdfsPath_(hdfsPath) {}

 private:
  std::string hdfsHost_;
  int32_t hdfsPort_;
  std::string hdfsPath_;
};

class Ingest final : public SingleDependencyNode {
 public:
  static Ingest* make(QueryContext* qctx, PlanNode* dep) {
    return qctx->objPool()->add(new Ingest(qctx, dep));
  }

 private:
  explicit Ingest(QueryContext* qctx, PlanNode* dep)
      : SingleDependencyNode(qctx, Kind::kIngest, dep) {}
};

// User related Node
class CreateUser final : public CreateNode {
 public:
  static CreateUser* make(QueryContext* qctx,
                          PlanNode* dep,
                          const std::string* username,
                          const std::string* password,
                          bool ifNotExists) {
    return qctx->objPool()->add(new CreateUser(qctx, dep, username, password, ifNotExists));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* password() const {
    return password_;
  }

 private:
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
    return qctx->objPool()->add(new DropUser(qctx, dep, username, ifNotExists));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
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
    return qctx->objPool()->add(new UpdateUser(qctx, dep, username, password));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

  const std::string* password() const {
    return password_;
  }

 private:
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
    return qctx->objPool()->add(new GrantRole(qctx, dep, username, spaceName, role));
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
    return qctx->objPool()->add(new RevokeRole(qctx, dep, username, spaceName, role));
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
    return qctx->objPool()->add(new ChangePassword(qctx, dep, username, password, newPassword));
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
    return qctx->objPool()->add(new ListUserRoles(qctx, dep, username));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
  ListUserRoles(QueryContext* qctx, PlanNode* dep, const std::string* username)
      : SingleDependencyNode(qctx, Kind::kListUserRoles, dep), username_(username) {}

 private:
  const std::string* username_;
};

class ListUsers final : public SingleDependencyNode {
 public:
  static ListUsers* make(QueryContext* qctx, PlanNode* dep) {
    return qctx->objPool()->add(new ListUsers(qctx, dep));
  }

 private:
  explicit ListUsers(QueryContext* qctx, PlanNode* dep)
      : SingleDependencyNode(qctx, Kind::kListUsers, dep) {}
};

class DescribeUser final : public SingleDependencyNode {
 public:
  static DescribeUser* make(QueryContext* qctx, PlanNode* dep, const std::string* username) {
    return qctx->objPool()->add(new DescribeUser(qctx, dep, username));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  const std::string* username() const {
    return username_;
  }

 private:
  explicit DescribeUser(QueryContext* qctx, PlanNode* dep, const std::string* username)
      : SingleDependencyNode(qctx, Kind::kDescribeUser, dep), username_(username) {}

  const std::string* username_;
};

class ListRoles final : public SingleDependencyNode {
 public:
  static ListRoles* make(QueryContext* qctx, PlanNode* dep, GraphSpaceID space) {
    return qctx->objPool()->add(new ListRoles(qctx, dep, space));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  GraphSpaceID space() const {
    return space_;
  }

 private:
  explicit ListRoles(QueryContext* qctx, PlanNode* dep, GraphSpaceID space)
      : SingleDependencyNode(qctx, Kind::kListRoles, dep), space_(space) {}

  GraphSpaceID space_{-1};
};

class ShowParts final : public SingleDependencyNode {
 public:
  static ShowParts* make(QueryContext* qctx,
                         PlanNode* input,
                         GraphSpaceID spaceId,
                         std::vector<PartitionID> partIds) {
    return qctx->objPool()->add(new ShowParts(qctx, input, spaceId, std::move(partIds)));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  GraphSpaceID getSpaceId() const {
    return spaceId_;
  }

  const std::vector<PartitionID>& getPartIds() const {
    return partIds_;
  }

 private:
  explicit ShowParts(QueryContext* qctx,
                     PlanNode* input,
                     GraphSpaceID spaceId,
                     std::vector<PartitionID> partIds)
      : SingleDependencyNode(qctx, Kind::kShowParts, input) {
    spaceId_ = spaceId;
    partIds_ = std::move(partIds);
  }

 private:
  GraphSpaceID spaceId_{-1};
  std::vector<PartitionID> partIds_;
};

class SubmitJob final : public SingleDependencyNode {
 public:
  static SubmitJob* make(QueryContext* qctx,
                         PlanNode* dep,
                         meta::cpp2::AdminJobOp op,
                         meta::cpp2::AdminCmd cmd,
                         const std::vector<std::string>& params) {
    return qctx->objPool()->add(new SubmitJob(qctx, dep, op, cmd, params));
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 public:
  meta::cpp2::AdminJobOp jobOp() const {
    return op_;
  }

  meta::cpp2::AdminCmd cmd() const {
    return cmd_;
  }

  const std::vector<std::string>& params() const {
    return params_;
  }

 private:
  SubmitJob(QueryContext* qctx,
            PlanNode* dep,
            meta::cpp2::AdminJobOp op,
            meta::cpp2::AdminCmd cmd,
            const std::vector<std::string>& params)
      : SingleDependencyNode(qctx, Kind::kSubmitJob, dep), op_(op), cmd_(cmd), params_(params) {}

 private:
  meta::cpp2::AdminJobOp op_;
  meta::cpp2::AdminCmd cmd_;
  const std::vector<std::string> params_;
};

class ShowCharset final : public SingleDependencyNode {
 public:
  static ShowCharset* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowCharset(qctx, input));
  }

 private:
  ShowCharset(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowCharset, input) {}
};

class ShowCollation final : public SingleDependencyNode {
 public:
  static ShowCollation* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowCollation(qctx, input));
  }

 private:
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
    return qctx->objPool()->add(
        new AddHostsIntoZone(qctx, input, std::move(zoneName), std::move(addresses), isNew));
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
    return qctx->objPool()->add(
        new MergeZone(qctx, input, std::move(zoneName), std::move(zoneNames)));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

  const std::vector<std::string>& zones() const {
    return zones_;
  }

 private:
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
    return qctx->objPool()->add(
        new RenameZone(qctx, input, std::move(originalZoneName), std::move(zoneName)));
  }

  const std::string& originalZoneName() const {
    return originalZoneName_;
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
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
    return qctx->objPool()->add(new DropZone(qctx, input, std::move(zoneName)));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
  DropZone(QueryContext* qctx, PlanNode* input, std::string zoneName)
      : SingleDependencyNode(qctx, Kind::kDropZone, input) {
    zoneName_ = std::move(zoneName);
  }

 private:
  std::string zoneName_;
};

class SplitZone final : public SingleDependencyNode {
 public:
  static SplitZone* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string zoneName,
                         std::vector<std::string> zoneNames) {
    return qctx->objPool()->add(
        new SplitZone(qctx, input, std::move(zoneName), std::move(zoneNames)));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

  const std::vector<std::string>& zones() const {
    return zones_;
  }

 private:
  SplitZone(QueryContext* qctx,
            PlanNode* input,
            std::string zoneName,
            std::vector<std::string> zoneNames)
      : SingleDependencyNode(qctx, Kind::kSplitZone, input) {
    zoneName_ = std::move(zoneName);
    zones_ = std::move(zoneNames);
  }

 private:
  std::string zoneName_;
  std::vector<std::string> zones_;
};

class DescribeZone final : public SingleDependencyNode {
 public:
  static DescribeZone* make(QueryContext* qctx, PlanNode* input, std::string zoneName) {
    return qctx->objPool()->add(new DescribeZone(qctx, input, std::move(zoneName)));
  }

  const std::string& zoneName() const {
    return zoneName_;
  }

 private:
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
    return qctx->objPool()->add(new ListZones(qctx, input));
  }

 private:
  ListZones(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowZones, input) {}
};

class ShowZones final : public SingleDependencyNode {
 public:
  static ShowZones* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowZones(qctx, input));
  }

 private:
  ShowZones(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowZones, input) {}
};

class ShowStats final : public SingleDependencyNode {
 public:
  static ShowStats* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowStats(qctx, input));
  }

 private:
  ShowStats(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowStats, input) {}
};

class ShowTSClients final : public SingleDependencyNode {
 public:
  static ShowTSClients* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new ShowTSClients(qctx, input));
  }

 private:
  ShowTSClients(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kShowTSClients, input) {}
};

class SignInTSService final : public SingleDependencyNode {
 public:
  static SignInTSService* make(QueryContext* qctx,
                               PlanNode* input,
                               std::vector<meta::cpp2::FTClient> clients) {
    return qctx->objPool()->add(new SignInTSService(qctx, input, std::move(clients)));
  }

  const std::vector<meta::cpp2::FTClient>& clients() const {
    return clients_;
  }

  meta::cpp2::FTServiceType type() const {
    return meta::cpp2::FTServiceType::ELASTICSEARCH;
  }

 private:
  SignInTSService(QueryContext* qctx, PlanNode* input, std::vector<meta::cpp2::FTClient> clients)
      : SingleDependencyNode(qctx, Kind::kSignInTSService, input), clients_(std::move(clients)) {}

  std::vector<meta::cpp2::FTClient> clients_;
};

class SignOutTSService final : public SingleDependencyNode {
 public:
  static SignOutTSService* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->add(new SignOutTSService(qctx, input));
  }

 private:
  SignOutTSService(QueryContext* qctx, PlanNode* input)
      : SingleDependencyNode(qctx, Kind::kSignOutTSService, input) {}
};

class ShowSessions final : public SingleInputNode {
 public:
  static ShowSessions* make(QueryContext* qctx,
                            PlanNode* input,
                            bool isSetSessionID,
                            SessionID sessionId) {
    return qctx->objPool()->add(new ShowSessions(qctx, input, isSetSessionID, sessionId));
  }

  bool isSetSessionID() const {
    return isSetSessionID_;
  }

  SessionID getSessionId() const {
    return sessionId_;
  }

 private:
  explicit ShowSessions(QueryContext* qctx,
                        PlanNode* input,
                        bool isSetSessionID,
                        SessionID sessionId)
      : SingleInputNode(qctx, Kind::kShowSessions, input) {
    isSetSessionID_ = isSetSessionID;
    sessionId_ = sessionId;
  }

 private:
  bool isSetSessionID_{false};
  SessionID sessionId_{-1};
};

class UpdateSession final : public SingleInputNode {
 public:
  static UpdateSession* make(QueryContext* qctx, PlanNode* input, meta::cpp2::Session session) {
    return qctx->objPool()->add(new UpdateSession(qctx, input, std::move(session)));
  }

  const meta::cpp2::Session& getSession() const {
    return session_;
  }

 private:
  explicit UpdateSession(QueryContext* qctx, PlanNode* input, meta::cpp2::Session session)
      : SingleInputNode(qctx, Kind::kUpdateSession, input), session_(std::move(session)) {}

 private:
  meta::cpp2::Session session_;
};

class ShowQueries final : public SingleInputNode {
 public:
  static ShowQueries* make(QueryContext* qctx, PlanNode* input, bool isAll) {
    return qctx->objPool()->add(new ShowQueries(qctx, input, isAll));
  }

  bool isAll() const {
    return isAll_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  explicit ShowQueries(QueryContext* qctx, PlanNode* input, bool isAll)
      : SingleInputNode(qctx, Kind::kShowQueries, input), isAll_(isAll) {}

  bool isAll_{false};
};

class KillQuery final : public SingleInputNode {
 public:
  static KillQuery* make(QueryContext* qctx,
                         PlanNode* input,
                         Expression* sessionId,
                         Expression* epId) {
    return qctx->objPool()->add(new KillQuery(qctx, input, sessionId, epId));
  }

  Expression* sessionId() const {
    return sessionId_;
  }

  Expression* epId() const {
    return epId_;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  explicit KillQuery(QueryContext* qctx, PlanNode* input, Expression* sessionId, Expression* epId)
      : SingleInputNode(qctx, Kind::kKillQuery, input), sessionId_(sessionId), epId_(epId) {}

  Expression* sessionId_;
  Expression* epId_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLAN_ADMIN_H_
