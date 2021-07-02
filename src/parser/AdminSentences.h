/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_ADMINSENTENCES_H_
#define PARSER_ADMINSENTENCES_H_

#include "parser/Clauses.h"
#include "parser/Sentence.h"
#include "parser/MutateSentences.h"
#include "common/network/NetworkUtils.h"

namespace nebula {

class ConfigRowItem;

class ShowHostsSentence : public Sentence {
public:
    ShowHostsSentence() {
        kind_ = Kind::kShowHosts;
    }

    explicit ShowHostsSentence(meta::cpp2::ListHostType type) : type_(type) {
        kind_ = Kind::kShowHosts;
    }

    std::string toString() const override;

    meta::cpp2::ListHostType getType() const {
        return type_;
    }

private:
    meta::cpp2::ListHostType type_;
};

class ShowSpacesSentence : public Sentence {
public:
    ShowSpacesSentence() {
        kind_ = Kind::kShowSpaces;
    }
    std::string toString() const override;
};

class ShowCreateSpaceSentence : public Sentence {
public:
    explicit ShowCreateSpaceSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowCreateSpace;
    }
    std::string toString() const override;

    const std::string* spaceName() const {
        return name_.get();
    }
private:
    std::unique_ptr<std::string>                name_;
};

class ShowPartsSentence : public Sentence {
public:
    ShowPartsSentence() {
        kind_ = Kind::kShowParts;
    }

    explicit ShowPartsSentence(std::vector<int32_t>* list) {
        list_.reset(list);
        kind_ = Kind::kShowParts;
    }

    std::vector<int32_t>* getList() const {
        return list_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::vector<int32_t>> list_;
};

class ShowUsersSentence : public Sentence {
public:
    ShowUsersSentence() {
        kind_ = Kind::kShowUsers;
    }
    std::string toString() const override;
};

class ShowRolesSentence : public Sentence {
public:
    explicit ShowRolesSentence(std::string *name) {
        name_.reset(name);
        kind_ = Kind::kShowRoles;
    }

    std::string toString() const override;

    const std::string *name() const {
        return name_.get();
    }

private:
    std::unique_ptr<std::string>          name_;
};

class ShowSnapshotsSentence : public Sentence {
public:
    ShowSnapshotsSentence() {
        kind_ = Kind::kShowSnapshots;
    }
    std::string toString() const override;
};

class ShowCharsetSentence final : public Sentence {
public:
    ShowCharsetSentence() {
        kind_ = Kind::kShowCharset;
    }
    std::string toString() const override;
};

class ShowCollationSentence final : public Sentence {
public:
    ShowCollationSentence() {
        kind_ = Kind::kShowCollation;
    }
    std::string toString() const override;
};

class ShowGroupsSentence final : public Sentence {
public:
    ShowGroupsSentence() {
        kind_ = Kind::kShowGroups;
    }
    std::string toString() const override;
};

class ShowZonesSentence final : public Sentence {
public:
    ShowZonesSentence() {
        kind_ = Kind::kShowZones;
    }
    std::string toString() const override;
};

class SpaceOptItem final {
public:
    using Value = boost::variant<int64_t, std::string, meta::cpp2::ColumnTypeDef>;

    enum OptionType : uint8_t {
        PARTITION_NUM,
        REPLICA_FACTOR,
        VID_TYPE,
        CHARSET,
        COLLATE,
        ATOMIC_EDGE,
        GROUP_NAME,
    };

    SpaceOptItem(OptionType op, std::string val) {
        optType_ = op;
        optValue_ = std::move(val);
    }

    SpaceOptItem(OptionType op, int64_t val) {
        optType_ = op;
        optValue_ = val;
    }

    SpaceOptItem(OptionType op, meta::cpp2::ColumnTypeDef val) {
        optType_ = op;
        optValue_ = std::move(val);
    }

    SpaceOptItem(OptionType op, bool val) {
        optType_ = op;
        optValue_ = val ? 1 : 0;
    }

    int64_t asInt() const {
        return boost::get<int64_t>(optValue_);
    }

    const std::string& asString() const {
        return boost::get<std::string>(optValue_);
    }

    const meta::cpp2::ColumnTypeDef& asTypeDef() const {
        return boost::get<meta::cpp2::ColumnTypeDef>(optValue_);
    }

    bool isInt() const {
        return optValue_.which() == 0;
    }

    bool isString() const {
        return optValue_.which() == 1;
    }

    bool isTypeDef() const {
        return optValue_.which() == 2;
    }

    int64_t getPartitionNum() const {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "partition_num value illegal.";
            return 0;
        }
    }

    int64_t getReplicaFactor() const {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "replica_factor value illegal.";
            return 0;
        }
    }

    meta::cpp2::ColumnTypeDef getVidType() const {
        if (isTypeDef()) {
            return asTypeDef();
        } else {
            LOG(ERROR) << "vid type illegal.";
            static meta::cpp2::ColumnTypeDef unknownTypeDef;
            unknownTypeDef.set_type(meta::cpp2::PropertyType::UNKNOWN);
            return unknownTypeDef;
        }
    }

    std::string getCharset() const {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "charset value illegal.";
            return "";
        }
    }

    std::string getCollate() const {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "collate value illage.";
            return "";
        }
    }

    std::string getGroupName() const {
        if (isString()) {
            return asString();
        } else {
            LOG(ERROR) << "group name value illage.";
            return "";
        }
    }

    OptionType getOptType() const {
        return optType_;
    }

    bool getAtomicEdge() const {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "atomic_edge value illegal.";
            return 0;
        }
    }

    bool isVidType() {
        return optType_ == OptionType::VID_TYPE;
    }

    std::string toString() const;

private:
    Value           optValue_;
    OptionType      optType_;
};


class SpaceOptList final {
public:
    void addOpt(SpaceOptItem *item) {
        items_.emplace_back(item);
    }

    std::vector<SpaceOptItem*> getOpts() const {
        std::vector<SpaceOptItem*> result;
        result.resize(items_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(items_.begin(), items_.end(), result.begin(), get);
        return result;
    }

    bool hasVidType() const {
        auto spaceOptItems = getOpts();
        for (SpaceOptItem* item : spaceOptItems) {
            if (item->isVidType()) {
                return true;
            }
        }
        return false;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<SpaceOptItem>>    items_;
};

class CreateSpaceSentence final : public CreateSentence {
public:
    CreateSpaceSentence(std::string* spaceName, bool ifNotExist)
        : CreateSentence(ifNotExist) {
        spaceName_.reset(spaceName);
        kind_ = Kind::kCreateSpace;
    }

    const std::string* spaceName() const {
        return spaceName_.get();
    }

    const std::string* groupName() const {
        return groupName_.get();
    }

    void setOpts(SpaceOptList* spaceOpts) {
        spaceOpts_.reset(spaceOpts);
    }

    void setGroupName(std::string* name) {
        groupName_.reset(name);
    }

    const SpaceOptList* spaceOpts() const {
        return spaceOpts_.get();
    }

    void setComment(std::string *name) {
        comment_.reset(name);
    }

    const std::string* comment() const {
        return comment_.get();
    }

    std::vector<SpaceOptItem*> getOpts() {
        if (spaceOpts_ == nullptr) {
            return {};
        }
        return spaceOpts_->getOpts();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>     spaceName_;
    std::unique_ptr<std::string>     groupName_;
    std::unique_ptr<SpaceOptList>    spaceOpts_;
    std::unique_ptr<std::string>     comment_;
};


class DropSpaceSentence final : public DropSentence {
public:
    DropSpaceSentence(std::string *spaceName, bool ifExist) : DropSentence(ifExist) {
        spaceName_.reset(spaceName);
        kind_ = Kind::kDropSpace;
    }

    void setClusterName(std::string* clusterName) {
        clusterName_.reset(clusterName);
    }

    const std::string* spaceName() const {
        return spaceName_.get();
    }

    const std::string* clusterName() const {
        return clusterName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>     spaceName_;
    std::unique_ptr<std::string>     clusterName_;
};


class DescribeSpaceSentence final : public Sentence {
public:
    explicit DescribeSpaceSentence(std::string *spaceName) {
        spaceName_.reset(spaceName);
        kind_ = Kind::kDescribeSpace;
    }

    const std::string* spaceName() const {
        return spaceName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>     spaceName_;
};

class ConfigRowItem {
public:
    explicit ConfigRowItem(meta::cpp2::ConfigModule module) {
        module_ = module;
    }

    ConfigRowItem(meta::cpp2::ConfigModule module, std::string* name, Expression* value) {
        module_ = module;
        name_.reset(name);
        value_ = value;
    }

    ConfigRowItem(meta::cpp2::ConfigModule module, std::string* name) {
        module_ = module;
        name_.reset(name);
    }

    ConfigRowItem(meta::cpp2::ConfigModule module, std::string* name, UpdateList *items) {
        module_ = module;
        name_.reset(name);
        updateItems_.reset(items);
    }

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

    const std::string* getName() const {
        return name_.get();
    }

    Expression* getValue() const {
        return value_;
    }

    const UpdateList* getUpdateItems() const {
        return updateItems_.get();
    }

    std::string toString() const;

private:
    meta::cpp2::ConfigModule        module_;
    std::unique_ptr<std::string>    name_;
    Expression*                     value_{nullptr};
    std::unique_ptr<UpdateList>     updateItems_;
};

class ConfigBaseSentence : public Sentence {
public:
    explicit ConfigBaseSentence(Kind kind, ConfigRowItem* item) {
        kind_ = kind;
        configItem_.reset(item);
    }

    ConfigRowItem* configItem() {
        return configItem_.get();
    }

protected:
    std::unique_ptr<ConfigRowItem>  configItem_;
};

class ShowConfigsSentence final : public ConfigBaseSentence {
public:
    explicit ShowConfigsSentence(ConfigRowItem* item)
        : ConfigBaseSentence(Kind::kShowConfigs, item) {}

    std::string toString() const override;
};

class SetConfigSentence final : public ConfigBaseSentence {
public:
    explicit SetConfigSentence(ConfigRowItem* item)
        : ConfigBaseSentence(Kind::kSetConfig, item) {}

    std::string toString() const override;
};

class GetConfigSentence final : public ConfigBaseSentence {
public:
    explicit GetConfigSentence(ConfigRowItem* item)
        : ConfigBaseSentence(Kind::kGetConfig, item) {}

    std::string toString() const override;
};

class BalanceSentence final : public Sentence {
public:
    enum class SubType : uint32_t {
        kUnknown,
        kLeader,
        kData,
        kDataStop,
        kDataReset,
        kShowBalancePlan,
    };

    // TODO: add more subtype for balance
    explicit BalanceSentence(SubType subType) {
        kind_ = Kind::kBalance;
        subType_ = std::move(subType);
    }

    explicit BalanceSentence(int64_t id) {
        kind_ = Kind::kBalance;
        subType_ = SubType::kShowBalancePlan;
        balanceId_ = id;
    }

    BalanceSentence(SubType subType, HostList *hostDel) {
        kind_ = Kind::kBalance;
        subType_ = std::move(subType);
        hostDel_.reset(hostDel);
    }

    std::string toString() const override;

    SubType subType() const {
        return subType_;
    }

    int64_t balanceId() const {
        return balanceId_;
    }

    HostList* hostDel() const {
        return hostDel_.get();
    }

private:
    SubType                         subType_{SubType::kUnknown};
    int64_t                         balanceId_{0};
    std::unique_ptr<HostList>       hostDel_;
};

class CreateSnapshotSentence final : public Sentence {
public:
    CreateSnapshotSentence() {
        kind_ = Kind::kCreateSnapshot;
    }

    std::string toString() const override;
};

class DropSnapshotSentence final : public Sentence {
public:
    explicit DropSnapshotSentence(std::string *name) {
        kind_ = Kind::kDropSnapshot;
        name_.reset(name);
    }

    const std::string* name() {
        return name_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>    name_;
};

class AddListenerSentence final : public Sentence {
public:
    AddListenerSentence(const meta::cpp2::ListenerType& type, HostList* hosts) {
        kind_ = Kind::kAddListener;
        type_ = type;
        listeners_.reset(hosts);
    }

    meta::cpp2::ListenerType type() const {
        return type_;
    }

    HostList* listeners() const {
        return listeners_.get();
    }

    std::string toString() const override;

private:
    meta::cpp2::ListenerType        type_;
    std::unique_ptr<HostList>       listeners_;
};

class RemoveListenerSentence final : public Sentence {
public:
    explicit RemoveListenerSentence(const meta::cpp2::ListenerType& type) {
        kind_ = Kind::kRemoveListener;
        type_ = type;
    }

    meta::cpp2::ListenerType type() const {
        return type_;
    }

    std::string toString() const override;

private:
    meta::cpp2::ListenerType        type_;
};

class ShowListenerSentence final : public Sentence {
public:
    ShowListenerSentence() {
        kind_ = Kind::kShowListener;
    }

    std::string toString() const override;
};

class AdminJobSentence final : public Sentence {
public:
    explicit AdminJobSentence(meta::cpp2::AdminJobOp op,
                              meta::cpp2::AdminCmd cmd = meta::cpp2::AdminCmd::UNKNOWN)
        : op_(op), cmd_(cmd) {
        kind_ = Kind::kAdminJob;
    }

    void addPara(const std::string& para);
    void addPara(const NameLabelList& NameLabelList);
    std::string toString() const override;
    meta::cpp2::AdminJobOp getOp() const;
    meta::cpp2::AdminCmd getCmd() const;
    const std::vector<std::string> &getParas() const;

private:
    meta::cpp2::AdminJobOp   op_;
    meta::cpp2::AdminCmd     cmd_;
    std::vector<std::string> paras_;
};

class ShowStatsSentence final : public Sentence {
public:
    ShowStatsSentence() {
        kind_ = Kind::kShowStats;
    }

    std::string toString() const override;
};

class TSClientList final {
public:
    void addClient(nebula::meta::cpp2::FTClient *client) {
        clients_.emplace_back(client);
    }

    std::string toString() const;

    std::vector<nebula::meta::cpp2::FTClient> clients() const {
        std::vector<nebula::meta::cpp2::FTClient> result;
        result.reserve(clients_.size());
        for (auto &client : clients_) {
            result.emplace_back(*client);
        }
        return result;
    }

private:
    std::vector<std::unique_ptr<nebula::meta::cpp2::FTClient>> clients_;
};

class ShowTSClientsSentence final : public Sentence {
public:
    ShowTSClientsSentence() {
        kind_ = Kind::kShowTSClients;
    }
    std::string toString() const override;
};

class SignInTextServiceSentence final : public Sentence {
public:
    explicit SignInTextServiceSentence(TSClientList *clients) {
        kind_ = Kind::kSignInTSService;
        clients_.reset(clients);
    }

    std::string toString() const override;

    TSClientList* clients() const {
        return clients_.get();
    }

private:
    std::unique_ptr<TSClientList>       clients_;
};

class SignOutTextServiceSentence final : public Sentence {
public:
    SignOutTextServiceSentence() {
        kind_ = Kind::kSignOutTSService;
    }

    std::string toString() const override;
};

class ShowSessionsSentence final : public Sentence {
public:
    ShowSessionsSentence() {
        kind_ = Kind::kShowSessions;
    }

    explicit ShowSessionsSentence(SessionID sessionId) {
        kind_ = Kind::kShowSessions;
        sessionId_ = sessionId;
        setSeesionId_ = true;
    }

    bool isSetSessionID() const {
        return setSeesionId_;
    }

    SessionID getSessionID() const {
        return sessionId_;
    }

    std::string toString() const override;

private:
    SessionID   sessionId_{0};
    bool        setSeesionId_{false};
};

class ShowQueriesSentence final : public Sentence {
public:
    explicit ShowQueriesSentence(bool isAll = false) {
        kind_ = Kind::kShowQueries;
        isAll_ = isAll;
    }

    bool isAll() const {
        return isAll_;
    }

    std::string toString() const override;

private:
    bool isAll_{false};
};

class QueryUniqueIdentifier final {
public:
    explicit QueryUniqueIdentifier(Expression* epId, Expression* sessionId)
        : epId_(epId), sessionId_(sessionId) {}

    Expression* sessionId() const {
        return sessionId_;
    }

    Expression* epId() const {
        return epId_;
    }

    void setSession() {
        isSetSession_ = true;
    }

    bool isSetSession() const {
        return isSetSession_;
    }

private:
    Expression* epId_{nullptr};
    Expression* sessionId_{nullptr};
    bool        isSetSession_{false};
};

class KillQuerySentence final : public Sentence {
public:
    explicit KillQuerySentence(QueryUniqueIdentifier* identifier) {
        kind_ = Kind::kKillQuery;
        identifier_.reset(identifier);
    }

    Expression* sessionId() const {
        return identifier_->sessionId();
    }

    Expression* epId() const {
        return identifier_->epId();
    }

    std::string toString() const override;

private:
    bool isSetSession() const {
        return identifier_->isSetSession();
    }

    std::unique_ptr<QueryUniqueIdentifier> identifier_;
};
}   // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
