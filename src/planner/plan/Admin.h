/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_ADMIN_H_
#define PLANNER_PLAN_ADMIN_H_

#include "planner/plan/Query.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/clients/meta/MetaClient.h"

/**
 * All admin-related nodes would be put in this file.
 * These nodes would not exist in a same plan with maintain-related/
 * mutate-related/query-related nodes. And they are also isolated
 * from each other. This would be guaranteed by parser and validator.
 */
namespace nebula {
namespace graph {

// Some template node such as Create template for the node create something(user,tag...)
// Fit the conflict create process
class CreateNode : public SingleDependencyNode {
protected:
    CreateNode(QueryContext* qctx,
               Kind kind,
               PlanNode* input,
               bool ifNotExist)
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

class CreateSpace final : public SingleDependencyNode {
public:
    static CreateSpace* make(QueryContext* qctx,
                             PlanNode* input,
                             meta::cpp2::SpaceDesc spaceDesc,
                             bool ifNotExists) {
        return qctx->objPool()->add(
            new CreateSpace(qctx, input, std::move(spaceDesc), ifNotExists));
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
    meta::cpp2::SpaceDesc     spaceDesc_;
    bool                      ifNotExists_{false};
};

class DropSpace final : public SingleDependencyNode {
public:
    static DropSpace* make(QueryContext* qctx,
                           PlanNode* input,
                           std::string spaceName,
                           bool ifExists) {
        return qctx->objPool()->add(new DropSpace(
            qctx, input, std::move(spaceName), ifExists));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    bool getIfExists() const {
        return ifExists_;
    }

private:
    DropSpace(QueryContext* qctx,
              PlanNode* input,
              std::string spaceName,
              bool ifExists)
        : SingleDependencyNode(qctx, Kind::kDropSpace, input) {
        spaceName_ = std::move(spaceName);
        ifExists_ = ifExists;
    }

private:
    std::string           spaceName_;
    bool                  ifExists_;
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
    DescSpace(QueryContext* qctx,
              PlanNode* input,
              std::string spaceName)
        : SingleDependencyNode(qctx, Kind::kDescSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
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
    ShowConfigs(QueryContext* qctx,
                PlanNode* input,
                meta::cpp2::ConfigModule module)
        : SingleDependencyNode(qctx, Kind::kShowConfigs, input)
        , module_(module) {}

private:
    meta::cpp2::ConfigModule    module_;
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
        return qctx->objPool()->add(
            new GetConfig(qctx, input, module, std::move(name)));
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
        return qctx->objPool()->add(
            new ShowCreateSpace(qctx, input, std::move(spaceName)));
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
        return qctx->objPool()->add(
            new DropSnapshot(qctx, input, std::move(snapshotName)));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const std::string& getShapshotName() const {
        return snapshotName_;
    }

private:
    explicit DropSnapshot(QueryContext* qctx,
                          PlanNode* input,
                          std::string snapshotName)
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
        return qctx->objPool()->add(
            new AddListener(qctx, input, std::move(type), std::move(hosts)));
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
    static RemoveListener* make(QueryContext* qctx,
                                PlanNode* input,
                                meta::cpp2::ListenerType type) {
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
    DropUser(QueryContext* qctx,
             PlanNode* dep,
             const std::string* username,
             bool ifNotExists)
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
        return qctx->objPool()->add(
            new RevokeRole(qctx, dep, username, spaceName, role));
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
        : SingleDependencyNode(qctx, Kind::kListUserRoles, dep),
          username_(username) {}

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
    GraphSpaceID                       spaceId_{-1};
    std::vector<PartitionID>           partIds_;
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

    const std::vector<std::string> &params() const {
        return params_;
    }

private:
    SubmitJob(QueryContext* qctx,
              PlanNode* dep,
              meta::cpp2::AdminJobOp op,
              meta::cpp2::AdminCmd cmd,
              const std::vector<std::string> &params)
        : SingleDependencyNode(qctx, Kind::kSubmitJob, dep),
          op_(op),
          cmd_(cmd),
          params_(params) {}


private:
    meta::cpp2::AdminJobOp         op_;
    meta::cpp2::AdminCmd           cmd_;
    const std::vector<std::string> params_;
};

class BalanceLeaders final : public SingleDependencyNode {
public:
    static BalanceLeaders* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new BalanceLeaders(qctx, dep));
    }

private:
    explicit BalanceLeaders(QueryContext* qctx, PlanNode* dep)
        : SingleDependencyNode(qctx, Kind::kBalanceLeaders, dep) {}
};

class Balance final : public SingleDependencyNode {
public:
    static Balance* make(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> deleteHosts) {
        return qctx->objPool()->add(new Balance(qctx, dep, std::move(deleteHosts)));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    const std::vector<HostAddr> &deleteHosts() const {
        return deleteHosts_;
    }

private:
    Balance(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> deleteHosts)
        : SingleDependencyNode(qctx, Kind::kBalance, dep),
          deleteHosts_(std::move(deleteHosts)) {}

    std::vector<HostAddr> deleteHosts_;
};

class StopBalance final : public SingleDependencyNode {
public:
    static StopBalance* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new StopBalance(qctx, dep));
    }

private:
    explicit StopBalance(QueryContext* qctx, PlanNode* dep)
        : SingleDependencyNode(qctx, Kind::kStopBalance, dep) {}
};

class ResetBalance final : public SingleDependencyNode {
public:
    static ResetBalance* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new ResetBalance(qctx, dep));
    }

private:
    explicit ResetBalance(QueryContext* qctx, PlanNode* dep)
        : SingleDependencyNode(qctx, Kind::kResetBalance, dep) {}
};

class ShowBalance final : public SingleDependencyNode {
public:
    static ShowBalance* make(QueryContext* qctx, PlanNode* dep, int64_t jobId) {
        return qctx->objPool()->add(new ShowBalance(qctx, dep, jobId));
    }

    std::unique_ptr<PlanNodeDescription> explain() const override;

    int64_t jobId() const {
        return jobId_;
    }

private:
    ShowBalance(QueryContext* qctx, PlanNode* dep, int64_t jobId)
        : SingleDependencyNode(qctx, Kind::kShowBalance, dep), jobId_(jobId) {}

    int64_t jobId_;
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

class AddGroup final : public SingleDependencyNode {
public:
    static AddGroup* make(QueryContext* qctx,
                          PlanNode* input,
                          std::string groupName,
                          std::vector<std::string> zoneNames) {
        return qctx->objPool()->add(new AddGroup(qctx,
                                                 input,
                                                 std::move(groupName),
                                                 std::move(zoneNames)));
    }

    const std::string& groupName() const {
        return groupName_;
    }

    const std::vector<std::string>& zoneNames() const {
        return zoneNames_;
    }

private:
    AddGroup(QueryContext* qctx,
             PlanNode* input,
             std::string groupName,
             std::vector<std::string> zoneNames)
        : SingleDependencyNode(qctx, Kind::kAddGroup, input) {
        groupName_ = std::move(groupName);
        zoneNames_ = std::move(zoneNames);
    }

private:
    std::string groupName_;
    std::vector<std::string> zoneNames_;
};

class DropGroup final : public SingleDependencyNode {
public:
    static DropGroup* make(QueryContext* qctx, PlanNode* input, std::string groupName) {
        return qctx->objPool()->add(new DropGroup(qctx, input, std::move(groupName)));
    }

    const std::string& groupName() const {
        return groupName_;
    }

private:
    DropGroup(QueryContext* qctx, PlanNode* input, std::string groupName)
        : SingleDependencyNode(qctx, Kind::kDropGroup, input) {
        groupName_ = std::move(groupName);
    }

private:
    std::string groupName_;
};

class DescribeGroup final : public SingleDependencyNode {
public:
    static DescribeGroup* make(QueryContext* qctx, PlanNode* input, std::string groupName) {
        return qctx->objPool()->add(new DescribeGroup(qctx, input, std::move(groupName)));
    }

    const std::string& groupName() const {
        return groupName_;
    }

private:
    DescribeGroup(QueryContext* qctx, PlanNode* input, std::string groupName)
        : SingleDependencyNode(qctx, Kind::kDescribeGroup, input) {
        groupName_ = std::move(groupName);
    }

private:
    std::string groupName_;
};

class ListGroups final : public SingleDependencyNode {
public:
    static ListGroups* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ListGroups(qctx, input));
    }

private:
    ListGroups(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowGroups, input) {}
};

class AddHostIntoZone final : public SingleDependencyNode {
public:
    static AddHostIntoZone* make(QueryContext* qctx,
                                 PlanNode* input,
                                 std::string zoneName,
                                 HostAddr addresses) {
        return qctx->objPool()->add(new AddHostIntoZone(qctx,
                                                        input,
                                                        std::move(zoneName),
                                                        std::move(addresses)));
    }

    const std::string& zoneName() const {
        return zoneName_;
    }

    const HostAddr& address() const {
        return addresses_;
    }

private:
    AddHostIntoZone(QueryContext* qctx,
                    PlanNode* input,
                    std::string zoneName,
                    HostAddr addresses)
        : SingleDependencyNode(qctx, Kind::kAddHostIntoZone, input) {
        zoneName_ = std::move(zoneName);
        addresses_ = std::move(addresses);
    }

private:
    std::string zoneName_;
    HostAddr    addresses_;
};

class DropHostFromZone final : public SingleDependencyNode {
public:
    static DropHostFromZone* make(QueryContext* qctx,
                                  PlanNode* input,
                                  std::string zoneName,
                                  HostAddr addresses) {
        return qctx->objPool()->add(new DropHostFromZone(qctx,
                                                         input,
                                                         std::move(zoneName),
                                                         std::move(addresses)));
    }

    const std::string& zoneName() const {
        return zoneName_;
    }

    const HostAddr& address() const {
        return addresses_;
    }

private:
    DropHostFromZone(QueryContext* qctx,
                     PlanNode* input,
                     std::string zoneName,
                     HostAddr addresses)
        : SingleDependencyNode(qctx, Kind::kDropHostFromZone, input) {
        zoneName_ = std::move(zoneName);
        addresses_ = std::move(addresses);
    }

private:
    std::string zoneName_;
    HostAddr    addresses_;
};

class AddZone final : public SingleDependencyNode {
public:
    static AddZone* make(QueryContext* qctx,
                         PlanNode* input,
                         std::string zoneName,
                         std::vector<HostAddr> addresses) {
        return qctx->objPool()->add(new AddZone(qctx,
                                                input,
                                                std::move(zoneName),
                                                std::move(addresses)));
    }

    const std::string& zoneName() const {
        return zoneName_;
    }

    const std::vector<HostAddr>& addresses() const {
        return addresses_;
    }

private:
    AddZone(QueryContext* qctx,
            PlanNode* input,
            std::string zoneName,
            std::vector<HostAddr> addresses)
        : SingleDependencyNode(qctx, Kind::kAddZone, input) {
        zoneName_ = std::move(zoneName);
        addresses_ = std::move(addresses);
    }

private:
    std::string zoneName_;
    std::vector<HostAddr> addresses_;
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

// class DrainZone final : public SingleDependencyNode {
// public:
//     static DrainZone* make(QueryContext* qctx, PlanNode* input, std::string zoneName) {
//         return qctx->objPool()->add(new DrainZone(qctx, input, std::move(zoneName)));
//     }

//     const std::string& zoneName() const {
//         return zoneName_;
//     }

// private:
//     DrainZone(QueryContext* qctx, PlanNode* input, std::string zoneName)
//         : SingleDependencyNode(qctx, Kind::kDrainZone, input) {
//         zoneName_ = std::move(zoneName);
//     }

// private:
//     std::string zoneName_;
// };

class ListZones final : public SingleDependencyNode {
public:
    static ListZones* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ListZones(qctx, input));
    }

private:
    ListZones(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowZones, input) {}
};

class AddZoneIntoGroup final : public SingleDependencyNode {
public:
    static AddZoneIntoGroup* make(QueryContext* qctx,
                                  PlanNode* input,
                                  std::string groupName,
                                  std::string zoneName) {
        return qctx->objPool()->add(new AddZoneIntoGroup(qctx,
                                                         input,
                                                         std::move(zoneName),
                                                         std::move(groupName)));
    }

    const std::string& zoneName() const {
        return zoneName_;
    }

    const std::string& groupName() const {
        return groupName_;
    }

private:
    AddZoneIntoGroup(QueryContext* qctx,
                    PlanNode* input,
                    std::string zoneName,
                    std::string groupName)
        : SingleDependencyNode(qctx, Kind::kAddZoneIntoGroup, input) {
        zoneName_ = std::move(zoneName);
        groupName_ = std::move(groupName);
    }

private:
    std::string    zoneName_;
    std::string    groupName_;
};

class DropZoneFromGroup final : public SingleDependencyNode {
public:
    static DropZoneFromGroup* make(QueryContext* qctx,
                                   PlanNode* input,
                                   std::string groupName,
                                   std::string zoneName) {
        return qctx->objPool()->add(new DropZoneFromGroup(qctx,
                                                          input,
                                                          std::move(zoneName),
                                                          std::move(groupName)));
    }

    const std::string& zoneName() const {
        return zoneName_;
    }

    const std::string& groupName() const {
        return groupName_;
    }

private:
    DropZoneFromGroup(QueryContext* qctx,
                      PlanNode* input,
                      std::string zoneName,
                      std::string groupName)
        : SingleDependencyNode(qctx, Kind::kDropZoneFromGroup, input) {
        zoneName_ = std::move(zoneName);
        groupName_ = std::move(groupName);
    }

private:
    std::string    zoneName_;
    std::string    groupName_;
};

class ShowGroups final : public SingleDependencyNode {
public:
    static ShowGroups* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowGroups(qctx, input));
    }

private:
    ShowGroups(QueryContext* qctx, PlanNode* input)
        : SingleDependencyNode(qctx, Kind::kShowGroups, input) {}
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

    const std::vector<meta::cpp2::FTClient> &clients() const {
        return clients_;
    }

    meta::cpp2::FTServiceType type() const {
        return meta::cpp2::FTServiceType::ELASTICSEARCH;
    }

private:
    SignInTSService(QueryContext* qctx, PlanNode* input, std::vector<meta::cpp2::FTClient> clients)
        : SingleDependencyNode(qctx, Kind::kSignInTSService, input),
          clients_(std::move(clients)) {}

    std::vector<meta::cpp2::FTClient> clients_;
};

class SignOutTSService final : public SingleDependencyNode {
public:
    static SignOutTSService* make(QueryContext* qctx,
                                  PlanNode* input) {
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
#endif  // PLANNER_PLAN_ADMIN_H_
