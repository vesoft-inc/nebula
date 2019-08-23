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
#include "network/NetworkUtils.h"

namespace nebula {

using nebula::network::NetworkUtils;

class ConfigRowItem;

class ShowSentence final : public Sentence {
public:
    enum class ShowType : uint32_t {
        kUnknown,
        kShowHosts,
        kShowSpaces,
        kShowTags,
        kShowEdges,
        kShowUsers,
        kShowUser,
        kShowRoles,
        kShowCreateSpace,
        kShowCreateTag,
        kShowCreateEdge
    };

    explicit ShowSentence(ShowType sType) {
        kind_ = Kind::kShow;
        showType_ = std::move(sType);
    }

    ShowSentence(ShowType sType, std::string *name) {
        kind_ = Kind::kShow;
        name_.reset(name);
        showType_ = std::move(sType);
    }

    std::string toString() const override;

    ShowType showType() const {
        return showType_;
    }

    std::string* getName() {
        return name_.get();
    }

private:
    ShowType                        showType_{ShowType::kUnknown};
    std::unique_ptr<std::string>    name_;
};


inline std::ostream& operator<<(std::ostream &os, ShowSentence::ShowType type) {
    return os << static_cast<uint32_t>(type);
}


class HostList final {
public:
    void addHost(HostAddr *addr) {
        hosts_.emplace_back(addr);
    }

    std::string toString() const;

    std::vector<HostAddr> hosts() const {
        std::vector<HostAddr> result;
        result.reserve(hosts_.size());
        for (auto &host : hosts_) {
            result.emplace_back(*host);
        }
        return result;
    }

private:
    std::vector<std::unique_ptr<HostAddr>>      hosts_;
};


class AddHostsSentence final : public Sentence {
public:
    AddHostsSentence() {
        kind_ = Kind::kAddHosts;
    }

    void setHosts(HostList *hosts) {
        hosts_.reset(hosts);
    }

    std::vector<HostAddr> hosts() const {
        return hosts_->hosts();
    }

    std::string toString() const override;

private:
    std::unique_ptr<HostList>               hosts_;
};


class RemoveHostsSentence final : public Sentence {
public:
    RemoveHostsSentence() {
        kind_ = Kind::kRemoveHosts;
    }

    void setHosts(HostList *hosts) {
        hosts_.reset(hosts);
    }

    std::vector<HostAddr> hosts() const {
        return hosts_->hosts();
    }

    std::string toString() const override;

private:
    std::unique_ptr<HostList>               hosts_;
};


class SpaceOptItem final {
public:
    using Value = boost::variant<int64_t, std::string>;

    enum OptionType : uint8_t {
        PARTITION_NUM, REPLICA_FACTOR
    };

    SpaceOptItem(OptionType op, std::string val) {
        optType_ = op;
        optValue_ = std::move(val);
    }

    SpaceOptItem(OptionType op, int64_t val) {
        optType_ = op;
        optValue_ = val;
    }

    int64_t asInt() {
        return boost::get<int64_t>(optValue_);
    }

    const std::string& asString() {
        return boost::get<std::string>(optValue_);
    }

    bool isInt() {
        return optValue_.which() == 0;
    }

    bool isString() {
        return optValue_.which() == 1;
    }

    int64_t get_partition_num() {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "partition_num value illegal.";
            return 0;
        }
    }

    int64_t get_replica_factor() {
        if (isInt()) {
            return asInt();
        } else {
            LOG(ERROR) << "replica_factor value illegal.";
            return 0;
        }
    }

    OptionType getOptType() {
        return optType_;
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

    std::string toString() const;

private:
    std::vector<std::unique_ptr<SpaceOptItem>>    items_;
};


class CreateSpaceSentence final : public Sentence {
public:
    explicit CreateSpaceSentence(std::string *spaceName) {
        spaceName_.reset(spaceName);
        kind_ = Kind::kCreateSpace;
    }

    const std::string* spaceName() const {
        return spaceName_.get();
    }

    void setOpts(SpaceOptList* spaceOpts) {
        spaceOpts_.reset(spaceOpts);
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
    std::unique_ptr<SpaceOptList>    spaceOpts_;
};


class DropSpaceSentence final : public Sentence {
public:
    explicit DropSpaceSentence(std::string *spaceName) {
        spaceName_.reset(spaceName);
        kind_ = Kind::kDropSpace;
    }

    const std::string* spaceName() const {
        return spaceName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>     spaceName_;
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

enum ConfigModule {
    ALL, GRAPH, META, STORAGE
};

class ConfigRowItem {
public:
    explicit ConfigRowItem(ConfigModule module) {
        module_ = std::make_unique<ConfigModule>(module);
    }

    ConfigRowItem(ConfigModule module, std::string* name, Expression* value) {
        module_ = std::make_unique<ConfigModule>(module);
        name_.reset(name);
        value_.reset(value);
    }

    ConfigRowItem(ConfigModule module, std::string* name) {
        module_ = std::make_unique<ConfigModule>(module);
        name_.reset(name);
    }

    ConfigRowItem(ConfigModule module, std::string* name, UpdateList *items) {
        module_ = std::make_unique<ConfigModule>(module);
        name_.reset(name);
        updateItems_.reset(items);
    }

    const ConfigModule* getModule() {
        return module_.get();
    }

    const std::string* getName() {
        return name_.get();
    }

    const Expression* getValue() {
        return value_.get();
    }

    const UpdateList* getUpdateItems() {
        return updateItems_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<ConfigModule>   module_;
    std::unique_ptr<std::string>    name_;
    std::unique_ptr<Expression>     value_;
    std::unique_ptr<UpdateList>     updateItems_;
};

class ConfigSentence final : public Sentence {
public:
    enum class SubType : uint32_t {
        kUnknown,
        kShow,
        kSet,
        kGet,
    };

    explicit ConfigSentence(SubType subType) {
        kind_ = Kind::kConfig;
        subType_ = std::move(subType);
    }

    ConfigSentence(SubType subType, ConfigRowItem* item) {
        kind_ = Kind::kConfig;
        subType_ = std::move(subType);
        configItem_.reset(item);
    }

    std::string toString() const override;

    SubType subType() const {
        return subType_;
    }

    ConfigRowItem* configItem() {
        return configItem_.get();
    }

private:
    SubType                         subType_{SubType::kUnknown};
    std::unique_ptr<ConfigRowItem>  configItem_;
};

class BalanceSentence final : public Sentence {
public:
    enum class SubType : uint32_t {
        kUnknown,
        kLeader,
        kData,
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

    std::string toString() const override;

    SubType subType() const {
        return subType_;
    }

    int64_t balanceId() const {
        return balanceId_;
    }

private:
    SubType                         subType_{SubType::kUnknown};
    int64_t                         balanceId_{0};
};

}   // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
