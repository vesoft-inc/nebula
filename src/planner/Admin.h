/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ADMIN_H_
#define PLANNER_ADMIN_H_

#include "planner/Query.h"
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
    CreateNode(ExecutionPlan* plan, Kind kind, PlanNode* input, bool ifNotExist = false)
        : SingleDependencyNode(plan, kind, input), ifNotExist_(ifNotExist) {}

public:
    bool ifNotExist() const {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

class DropNode : public SingleDependencyNode {
protected:
    DropNode(ExecutionPlan* plan, Kind kind, PlanNode* input, bool ifExist = false)
        : SingleDependencyNode(plan, kind, input), ifExist_(ifExist) {}

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
    static ShowHosts* make(ExecutionPlan* plan, PlanNode* dep) {
        return new ShowHosts(plan, dep);
    }

private:
    explicit ShowHosts(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kShowHosts, dep) {}
};

class CreateSpace final : public SingleInputNode {
public:
    static CreateSpace* make(ExecutionPlan* plan,
                             PlanNode* input,
                             meta::SpaceDesc props,
                             bool ifNotExists) {
    return new CreateSpace(plan,
                           input,
                           std::move(props),
                           ifNotExists);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    const meta::SpaceDesc& getSpaceDesc() const {
        return props_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

private:
    CreateSpace(ExecutionPlan* plan,
                PlanNode* input,
                meta::SpaceDesc props,
                bool ifNotExists)
        : SingleInputNode(plan, Kind::kCreateSpace, input) {
        props_ = std::move(props);
        ifNotExists_ = ifNotExists;
    }

private:
    meta::SpaceDesc     props_;
    bool                ifNotExists_{false};
};

class DropSpace final : public SingleInputNode {
public:
    static DropSpace* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName,
                           bool ifExists) {
        return new DropSpace(plan, input, std::move(spaceName), ifExists);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    bool getIfExists() const {
        return ifExists_;
    }

private:
    DropSpace(ExecutionPlan* plan,
              PlanNode* input,
              std::string spaceName,
              bool ifExists)
        : SingleInputNode(plan, Kind::kDropSpace, input) {
        spaceName_ = std::move(spaceName);
        ifExists_ = ifExists;
    }

private:
    std::string           spaceName_;
    bool                  ifExists_;
};

class DescSpace final : public SingleInputNode {
public:
    static DescSpace* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName) {
    return new DescSpace(plan, input, std::move(spaceName));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    DescSpace(ExecutionPlan* plan,
              PlanNode* input,
              std::string spaceName)
        : SingleInputNode(plan, Kind::kDescSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
};

class ShowSpaces final : public SingleInputNode {
public:
    static ShowSpaces* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowSpaces(plan, input);
    }

private:
    explicit ShowSpaces(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowSpaces, input) {}
};

class ShowConfigs final : public SingleInputNode {
public:
    static ShowConfigs* make(ExecutionPlan* plan,
                             PlanNode* input,
                             meta::cpp2::ConfigModule module) {
        return new ShowConfigs(plan, input, module);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

private:
    ShowConfigs(ExecutionPlan* plan,
                PlanNode* input,
                meta::cpp2::ConfigModule module)
        : SingleInputNode(plan, Kind::kShowConfigs, input)
        , module_(module) {}

private:
    meta::cpp2::ConfigModule    module_;
};

class SetConfig final : public SingleInputNode {
public:
    static SetConfig* make(ExecutionPlan* plan,
                           PlanNode* input,
                           meta::cpp2::ConfigModule module,
                           std::string name,
                           Value value) {
        return new SetConfig(plan, input, module, std::move(name), std::move(value));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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
    SetConfig(ExecutionPlan* plan,
              PlanNode* input,
              meta::cpp2::ConfigModule module,
              std::string name,
              Value value)
        : SingleInputNode(plan, Kind::kSetConfig, input)
        , module_(module)
        , name_(std::move(name))
        , value_(std::move(value)) {}

private:
    meta::cpp2::ConfigModule       module_;
    std::string                    name_;
    Value                          value_;
};

class GetConfig final : public SingleInputNode {
public:
    static GetConfig* make(ExecutionPlan* plan,
                           PlanNode* input,
                           meta::cpp2::ConfigModule module,
                           std::string name) {
        return new GetConfig(plan, input, module, std::move(name));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

    const std::string& getName() const {
        return name_;
    }

private:
    explicit GetConfig(ExecutionPlan* plan,
                       PlanNode* input,
                       meta::cpp2::ConfigModule module,
                       std::string name)
        : SingleInputNode(plan, Kind::kGetConfig, input)
        , module_(module)
        , name_(std::move(name)) {}

private:
    meta::cpp2::ConfigModule       module_;
    std::string                    name_;
};

class ShowCreateSpace final : public SingleInputNode {
public:
    static ShowCreateSpace* make(ExecutionPlan* plan,
                                 PlanNode* input,
                                 std::string spaceName) {
        return new ShowCreateSpace(plan, input, std::move(spaceName));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    ShowCreateSpace(ExecutionPlan* plan,
                    PlanNode* input,
                    std::string spaceName)
        : SingleInputNode(plan, Kind::kShowCreateSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
};

class CreateSnapshot final : public SingleInputNode {
public:
    static CreateSnapshot* make(ExecutionPlan* plan, PlanNode* input) {
        return new CreateSnapshot(plan, input);
    }

private:
    explicit CreateSnapshot(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kCreateSnapshot, input) {}
};

class DropSnapshot final : public SingleInputNode {
public:
    static DropSnapshot* make(ExecutionPlan* plan,
                              PlanNode* input,
                              std::string snapshotName) {
        return new DropSnapshot(plan, input, std::move(snapshotName));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getShapshotName() const {
        return snapshotName_;
    }

private:
    explicit DropSnapshot(ExecutionPlan* plan,
                          PlanNode* input,
                          std::string snapshotName)
        : SingleInputNode(plan, Kind::kDropSnapshot, input) {
        snapshotName_ = std::move(snapshotName);
    }

private:
    std::string           snapshotName_;
};

class ShowSnapshots final : public SingleInputNode {
public:
    static ShowSnapshots* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowSnapshots(plan, input);
    }

private:
    explicit ShowSnapshots(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowSnapshots, input) {}
};

class Download final : public SingleInputNode {
public:
};

class Ingest final : public SingleInputNode {
public:
};

// User related Node
class CreateUser final : public CreateNode {
public:
    static CreateUser* make(ExecutionPlan*     plan,
                            PlanNode*          dep,
                            const std::string* username,
                            const std::string* password,
                            bool ifNotExists) {
        return new CreateUser(plan,
                              dep,
                              username,
                              password,
                              ifNotExists);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    CreateUser(ExecutionPlan* plan,
               PlanNode* dep,
               const std::string* username,
               const std::string* password,
               bool ifNotExists)
        : CreateNode(plan, Kind::kCreateUser, dep, ifNotExists),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class DropUser final : public DropNode {
public:
    static DropUser* make(ExecutionPlan*     plan,
                          PlanNode*          dep,
                          const std::string* username,
                          bool ifNotExists) {
        return new DropUser(plan,
                            dep,
                            username,
                            ifNotExists);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    const std::string* username() const {
        return username_;
    }

private:
    DropUser(ExecutionPlan* plan, PlanNode* dep, const std::string* username, bool ifNotExists)
        : DropNode(plan, Kind::kDropUser, dep, ifNotExists),
          username_(username) {}

private:
    const std::string* username_;
};

class UpdateUser final : public SingleDependencyNode {
public:
    static UpdateUser* make(ExecutionPlan*     plan,
                            PlanNode*          dep,
                            const std::string* username,
                            const std::string* password) {
        return new UpdateUser(plan,
                              dep,
                              username,
                              password);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    UpdateUser(ExecutionPlan* plan,
               PlanNode* dep,
               const std::string* username,
               const std::string* password)
        : SingleDependencyNode(plan, Kind::kUpdateUser, dep),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class GrantRole final : public SingleDependencyNode {
public:
    static GrantRole* make(ExecutionPlan* plan,
                           PlanNode*      dep,
                           const std::string* username,
                           const std::string* spaceName,
                           meta::cpp2::RoleType role) {
        return new GrantRole(plan,
                             dep,
                             username,
                             spaceName,
                             role);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

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
    GrantRole(ExecutionPlan* plan,
              PlanNode* dep,
              const std::string* username,
              const std::string* spaceName,
              meta::cpp2::RoleType role)
        : SingleDependencyNode(plan, Kind::kGrantRole, dep),
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
    static RevokeRole* make(ExecutionPlan* plan,
                            PlanNode*      dep,
                            const std::string* username,
                            const std::string* spaceName,
                            meta::cpp2::RoleType role) {
        return new RevokeRole(plan,
                              dep,
                              username,
                              spaceName,
                              role);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

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
    RevokeRole(ExecutionPlan* plan,
               PlanNode*      dep,
               const std::string* username,
               const std::string* spaceName,
               meta::cpp2::RoleType role)
        : SingleDependencyNode(plan, Kind::kRevokeRole, dep),
          username_(username),
          spaceName_(spaceName),
          role_(role) {}

private:
    const std::string*          username_;
    const std::string*          spaceName_;
    meta::cpp2::RoleType role_;
};

class ChangePassword final : public SingleDependencyNode {
public:
    static ChangePassword* make(ExecutionPlan*     plan,
                                PlanNode*          dep,
                                const std::string* username,
                                const std::string* password,
                                const std::string* newPassword) {
        return new ChangePassword(plan,
                                  dep,
                                  username,
                                  password,
                                  newPassword);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

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
    ChangePassword(ExecutionPlan* plan,
                   PlanNode* dep,
                   const std::string* username,
                   const std::string* password,
                   const std::string* newPassword)
        : SingleDependencyNode(plan, Kind::kChangePassword, dep),
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
    static ListUserRoles* make(ExecutionPlan*     plan,
                               PlanNode*          dep,
                               const std::string* username) {
        return new ListUserRoles(plan,
                                 dep,
                                 username);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    const std::string* username() const {
        return username_;
    }

private:
    ListUserRoles(ExecutionPlan* plan, PlanNode* dep, const std::string* username)
        : SingleDependencyNode(plan, Kind::kListUserRoles, dep),
          username_(username) {}

private:
    const std::string* username_;
};

class ListUsers final : public SingleDependencyNode {
public:
    static ListUsers* make(ExecutionPlan* plan, PlanNode* dep) {
        return new ListUsers(plan, dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

private:
    explicit ListUsers(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kListUsers, dep) {}
};

class ListRoles final : public SingleDependencyNode {
public:
    static ListRoles* make(ExecutionPlan* plan, PlanNode* dep, GraphSpaceID space) {
        return new ListRoles(plan, dep, space);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    GraphSpaceID space() const {
        return space_;
    }

private:
    explicit ListRoles(ExecutionPlan* plan, PlanNode* dep, GraphSpaceID space)
        : SingleDependencyNode(plan, Kind::kListRoles, dep), space_(space) {}

    GraphSpaceID space_{-1};
};

class ShowParts final : public SingleInputNode {
public:
    static ShowParts* make(ExecutionPlan* plan,
                           PlanNode* input,
                           GraphSpaceID spaceId,
                           std::vector<PartitionID> partIds) {
        return new ShowParts(plan, input, spaceId, std::move(partIds));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    GraphSpaceID getSpaceId() const {
        return spaceId_;
    }

    const std::vector<PartitionID>& getPartIds() const {
        return partIds_;
    }

private:
    explicit ShowParts(ExecutionPlan* plan,
                       PlanNode* input,
                       GraphSpaceID spaceId,
                       std::vector<PartitionID> partIds)
        : SingleInputNode(plan, Kind::kShowParts, input) {
        spaceId_ = spaceId;
        partIds_ = std::move(partIds);
    }

private:
    GraphSpaceID                       spaceId_{-1};
    std::vector<PartitionID>           partIds_;
};

class SubmitJob final : public SingleDependencyNode {
public:
    static SubmitJob* make(ExecutionPlan* plan,
                           PlanNode*      dep,
                           meta::cpp2::AdminJobOp op,
                           const std::vector<std::string>& params) {
        return new SubmitJob(plan, dep, op, params);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        // TODO(shylock)
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

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
    SubmitJob(ExecutionPlan* plan,
              PlanNode* dep,
              meta::cpp2::AdminJobOp op,
              const std::vector<std::string> &params)
        : SingleDependencyNode(plan, Kind::kSubmitJob, dep),
          op_(op),
          params_(params) {}


private:
    meta::cpp2::AdminJobOp         op_;
    meta::cpp2::AdminCmd           cmd_;
    const std::vector<std::string> params_;
};

class BalanceLeaders final : public SingleDependencyNode {
public:
    static BalanceLeaders* make(ExecutionPlan* plan, PlanNode* dep) {
        return new BalanceLeaders(plan, dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

private:
    explicit BalanceLeaders(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kBalanceLeaders, dep) {}
};

class Balance final : public SingleDependencyNode {
public:
    static Balance* make(ExecutionPlan* plan, PlanNode* dep, std::vector<HostAddr> deleteHosts) {
        return new Balance(plan, dep, std::move(deleteHosts));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    const std::vector<HostAddr> &deleteHosts() const {
        return deleteHosts_;
    }

private:
    Balance(ExecutionPlan* plan, PlanNode* dep, std::vector<HostAddr> deleteHosts)
        : SingleDependencyNode(plan, Kind::kBalance, dep), deleteHosts_(std::move(deleteHosts)) {}

    std::vector<HostAddr> deleteHosts_;
};

class StopBalance final : public SingleDependencyNode {
public:
    static StopBalance* make(ExecutionPlan* plan, PlanNode* dep) {
        return new StopBalance(plan, dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

private:
    explicit StopBalance(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kStopBalance, dep) {}
};

class ShowBalance final : public SingleDependencyNode {
public:
    static ShowBalance* make(ExecutionPlan* plan, PlanNode* dep, int64_t id) {
        return new ShowBalance(plan, dep, id);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        LOG(FATAL) << "Unimplemented";
        return nullptr;
    }

    int64_t id() const {
        return id_;
    }

private:
    ShowBalance(ExecutionPlan* plan, PlanNode* dep, int64_t id)
        : SingleDependencyNode(plan, Kind::kShowBalance, dep), id_(id) {}

    int64_t id_;
};

class ShowCharset final : public SingleInputNode {
public:
    static ShowCharset* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowCharset(plan, input);
    }

private:
    explicit ShowCharset(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowCharset, input) {}
};

class ShowCollation final : public SingleInputNode {
public:
    static ShowCollation* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowCollation(plan, input);
    }

private:
    explicit ShowCollation(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowCollation, input) {}
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_

