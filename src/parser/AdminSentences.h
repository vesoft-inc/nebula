/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef PARSER_ADMINSENTENCES_H_
#define PARSER_ADMINSENTENCES_H_

#include "common/network/NetworkUtils.h"
#include "parser/Clauses.h"
#include "parser/MutateSentences.h"
#include "parser/Sentence.h"

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

class ShowMetaLeaderSentence : public Sentence {
 public:
  ShowMetaLeaderSentence() {
    kind_ = Kind::kShowMetaLeader;
  }

  std::string toString() const override;
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
  explicit ShowCreateSpaceSentence(std::string* name) {
    name_.reset(name);
    kind_ = Kind::kShowCreateSpace;
  }
  std::string toString() const override;

  const std::string* spaceName() const {
    return name_.get();
  }

 private:
  std::unique_ptr<std::string> name_;
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

class DescribeUserSentence : public Sentence {
 public:
  explicit DescribeUserSentence(std::string* account) {
    account_.reset(account);
    kind_ = Kind::kDescribeUser;
  }

  std::string toString() const override;

  const std::string* account() const {
    return account_.get();
  }

 private:
  std::unique_ptr<std::string> account_;
};

class ShowRolesSentence : public Sentence {
 public:
  explicit ShowRolesSentence(std::string* name) {
    name_.reset(name);
    kind_ = Kind::kShowRoles;
  }

  std::string toString() const override;

  const std::string* name() const {
    return name_.get();
  }

 private:
  std::unique_ptr<std::string> name_;
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

class SpaceOptItem final {
 public:
  using Value = std::variant<int64_t, std::string, meta::cpp2::ColumnTypeDef>;

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
    return std::get<int64_t>(optValue_);
  }

  const std::string& asString() const {
    return std::get<std::string>(optValue_);
  }

  const meta::cpp2::ColumnTypeDef& asTypeDef() const {
    return std::get<meta::cpp2::ColumnTypeDef>(optValue_);
  }

  bool isInt() const {
    return optValue_.index() == 0;
  }

  bool isString() const {
    return optValue_.index() == 1;
  }

  bool isTypeDef() const {
    return optValue_.index() == 2;
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
      unknownTypeDef.type_ref() = nebula::cpp2::PropertyType::UNKNOWN;
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
      LOG(ERROR) << "collate value illegal.";
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
  Value optValue_;
  OptionType optType_;
};

class SpaceOptList final {
 public:
  void addOpt(SpaceOptItem* item) {
    items_.emplace_back(item);
  }

  std::vector<SpaceOptItem*> getOpts() const {
    std::vector<SpaceOptItem*> result;
    result.resize(items_.size());
    auto get = [](auto& ptr) { return ptr.get(); };
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
  std::vector<std::unique_ptr<SpaceOptItem>> items_;
};

class CreateSpaceSentence final : public CreateSentence {
 public:
  CreateSpaceSentence(std::string* spaceName, bool ifNotExist) : CreateSentence(ifNotExist) {
    spaceName_.reset(spaceName);
    kind_ = Kind::kCreateSpace;
  }

  const std::string* spaceName() const {
    return spaceName_.get();
  }

  const ZoneNameList* zoneNames() const {
    return zoneNames_.get();
  }

  void setOpts(SpaceOptList* spaceOpts) {
    spaceOpts_.reset(spaceOpts);
  }

  void setZoneNames(ZoneNameList* names) {
    zoneNames_.reset(names);
  }

  const SpaceOptList* spaceOpts() const {
    return spaceOpts_.get();
  }

  void setComment(std::string* name) {
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
  std::unique_ptr<std::string> spaceName_;
  std::unique_ptr<ZoneNameList> zoneNames_;
  std::unique_ptr<SpaceOptList> spaceOpts_;
  std::unique_ptr<std::string> comment_;
};

class CreateSpaceAsSentence final : public CreateSentence {
 public:
  CreateSpaceAsSentence(std::string* oldSpace, std::string* newSpace, bool ifNotExist)
      : CreateSentence(ifNotExist) {
    oldSpaceName_.reset(oldSpace);
    newSpaceName_.reset(newSpace);
    kind_ = Kind::kCreateSpaceAs;
  }

  std::string getOldSpaceName() const {
    return *oldSpaceName_;
  }

  std::string getNewSpaceName() const {
    return *newSpaceName_;
  }

  std::string toString() const override;

 private:
  std::unique_ptr<std::string> newSpaceName_;
  std::unique_ptr<std::string> oldSpaceName_;
};

class DropSpaceSentence final : public DropSentence {
 public:
  DropSpaceSentence(std::string* spaceName, bool ifExist) : DropSentence(ifExist) {
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
  std::unique_ptr<std::string> spaceName_;
  std::unique_ptr<std::string> clusterName_;
};

// clear space data and index data, but keep space schema and index schema.
class ClearSpaceSentence final : public DropSentence {
 public:
  ClearSpaceSentence(std::string* spaceName, bool ifExist) : DropSentence(ifExist) {
    spaceName_.reset(spaceName);
    kind_ = Kind::kClearSpace;
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
  std::unique_ptr<std::string> spaceName_;
  std::unique_ptr<std::string> clusterName_;
};

class AlterSpaceSentence final : public Sentence {
 public:
  AlterSpaceSentence(std::string* spaceName, meta::cpp2::AlterSpaceOp op)
      : op_(op), spaceName_(spaceName) {
    kind_ = Kind::kAlterSpace;
  }
  void addPara(const std::string& para) {
    paras_.push_back(para);
  }

  std::string spaceName() const {
    return *spaceName_;
  }

  const std::vector<std::string>& paras() const {
    return paras_;
  }

  meta::cpp2::AlterSpaceOp alterSpaceOp() const {
    return op_;
  }

  std::string toString() const override;

 private:
  meta::cpp2::AlterSpaceOp op_;
  std::unique_ptr<std::string> spaceName_;
  std::vector<std::string> paras_;
};

class DescribeSpaceSentence final : public Sentence {
 public:
  explicit DescribeSpaceSentence(std::string* spaceName) {
    spaceName_.reset(spaceName);
    kind_ = Kind::kDescribeSpace;
  }

  const std::string* spaceName() const {
    return spaceName_.get();
  }

  std::string toString() const override;

 private:
  std::unique_ptr<std::string> spaceName_;
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

  ConfigRowItem(meta::cpp2::ConfigModule module, std::string* name, UpdateList* items) {
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
  meta::cpp2::ConfigModule module_;
  std::unique_ptr<std::string> name_;
  Expression* value_{nullptr};
  std::unique_ptr<UpdateList> updateItems_;
};

class ConfigBaseSentence : public Sentence {
 public:
  ConfigBaseSentence(Kind kind, ConfigRowItem* item) {
    kind_ = kind;
    configItem_.reset(item);
  }

  ConfigRowItem* configItem() {
    return configItem_.get();
  }

 protected:
  std::unique_ptr<ConfigRowItem> configItem_;
};

class ShowConfigsSentence final : public ConfigBaseSentence {
 public:
  explicit ShowConfigsSentence(ConfigRowItem* item)
      : ConfigBaseSentence(Kind::kShowConfigs, item) {}

  std::string toString() const override;
};

class SetConfigSentence final : public ConfigBaseSentence {
 public:
  explicit SetConfigSentence(ConfigRowItem* item) : ConfigBaseSentence(Kind::kSetConfig, item) {}

  std::string toString() const override;
};

class GetConfigSentence final : public ConfigBaseSentence {
 public:
  explicit GetConfigSentence(ConfigRowItem* item) : ConfigBaseSentence(Kind::kGetConfig, item) {}

  std::string toString() const override;
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
  explicit DropSnapshotSentence(std::string* name) {
    kind_ = Kind::kDropSnapshot;
    name_.reset(name);
  }

  const std::string* name() {
    return name_.get();
  }

  std::string toString() const override;

 private:
  std::unique_ptr<std::string> name_;
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
  meta::cpp2::ListenerType type_;
  std::unique_ptr<HostList> listeners_;
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
  meta::cpp2::ListenerType type_;
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
  explicit AdminJobSentence(meta::cpp2::JobOp op,
                            meta::cpp2::JobType type = meta::cpp2::JobType::UNKNOWN)
      : op_(op), type_(type) {
    if (op == meta::cpp2::JobOp::SHOW || op == meta::cpp2::JobOp::SHOW_All) {
      kind_ = Kind::kAdminShowJobs;
    } else {
      kind_ = Kind::kAdminJob;
    }
  }

  void addPara(const std::string& para);
  void addPara(const NameLabelList& NameLabelList);
  std::string toString() const override;
  meta::cpp2::JobOp getOp() const;
  meta::cpp2::JobType getJobType() const;
  const std::vector<std::string>& getParas() const;

  bool needWriteSpace() const;

 private:
  meta::cpp2::JobOp op_;
  meta::cpp2::JobType type_;
  std::vector<std::string> paras_;
};

class ShowStatsSentence final : public Sentence {
 public:
  ShowStatsSentence() {
    kind_ = Kind::kShowStats;
  }

  std::string toString() const override;
};

class ServiceClientList final {
 public:
  void addClient(nebula::meta::cpp2::ServiceClient* client) {
    clients_.emplace_back(client);
  }

  std::string toString() const;

  std::vector<nebula::meta::cpp2::ServiceClient> clients() const {
    std::vector<nebula::meta::cpp2::ServiceClient> result;
    result.reserve(clients_.size());
    for (auto& client : clients_) {
      result.emplace_back(*client);
    }
    return result;
  }

 private:
  std::vector<std::unique_ptr<nebula::meta::cpp2::ServiceClient>> clients_;
};

class ShowServiceClientsSentence final : public Sentence {
 public:
  explicit ShowServiceClientsSentence(const meta::cpp2::ExternalServiceType& type) : type_(type) {
    kind_ = Kind::kShowServiceClients;
  }

  std::string toString() const override;

  meta::cpp2::ExternalServiceType getType() {
    return type_;
  }

 private:
  meta::cpp2::ExternalServiceType type_;
};

class SignInServiceSentence final : public Sentence {
 public:
  SignInServiceSentence(const meta::cpp2::ExternalServiceType& type, ServiceClientList* clients)
      : type_(type) {
    kind_ = Kind::kSignInService;
    clients_.reset(clients);
  }

  std::string toString() const override;

  ServiceClientList* clients() const {
    return clients_.get();
  }

  meta::cpp2::ExternalServiceType getType() {
    return type_;
  }

 private:
  std::unique_ptr<ServiceClientList> clients_;
  meta::cpp2::ExternalServiceType type_;
};

class SignOutServiceSentence final : public Sentence {
 public:
  explicit SignOutServiceSentence(const meta::cpp2::ExternalServiceType& type) : type_(type) {
    kind_ = Kind::kSignOutService;
  }

  std::string toString() const override;

  meta::cpp2::ExternalServiceType getType() {
    return type_;
  }

 private:
  meta::cpp2::ExternalServiceType type_;
};

class ShowSessionsSentence final : public Sentence {
 public:
  ShowSessionsSentence() {
    kind_ = Kind::kShowSessions;
  }

  explicit ShowSessionsSentence(SessionID sessionId) {
    kind_ = Kind::kShowSessions;
    sessionId_ = sessionId;
    setSessionId_ = true;
  }

  explicit ShowSessionsSentence(bool isLocalCommand) {
    kind_ = Kind::kShowSessions;
    isLocalCommand_ = isLocalCommand;
  }

  bool isSetSessionID() const {
    return setSessionId_;
  }

  bool isLocalCommand() const {
    return isLocalCommand_;
  }

  SessionID getSessionID() const {
    return sessionId_;
  }

  std::string toString() const override;

 private:
  SessionID sessionId_{0};
  bool setSessionId_{false};
  bool isLocalCommand_{false};
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
  QueryUniqueIdentifier(Expression* epId, Expression* sessionId)
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
  bool isSetSession_{false};
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

class KillSessionSentence final : public Sentence {
 public:
  explicit KillSessionSentence(Expression* sessionId) {
    kind_ = Kind::kKillSession;
    sessionId_ = sessionId;
  }

  Expression* getSessionID() const {
    return sessionId_;
  }

  std::string toString() const override;

 private:
  Expression* sessionId_{nullptr};
};

class TransferLeaderSentence final : public Sentence {
 public:
  explicit TransferLeaderSentence(HostAddr *address, std::string* spaceName, int32_t concurrency) {
    address_.reset(address);
    spaceName_.reset(spaceName);
    concurrency_ = concurrency;
    kind_ = Kind::kTransferLeader;
  }

  const std::string *spaceName() const { return spaceName_.get(); }

  const HostAddr *address() const { return address_.get(); }

  int32_t concurrency() const { return concurrency_; }

  std::string toString() const override;

 private:
  std::unique_ptr<std::string> spaceName_;
  std::unique_ptr<HostAddr> address_;
  int32_t concurrency_;
};

}  // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
