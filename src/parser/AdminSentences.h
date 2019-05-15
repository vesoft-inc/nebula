/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_ADMINSENTENCES_H_
#define PARSER_ADMINSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"
#include "network/NetworkUtils.h"

namespace nebula {

using nebula::network::NetworkUtils;

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
        kShowRoles
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
    ShowType                      showType_{ShowType::kUnknown};
    std::unique_ptr<std::string>  name_;
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

    std::vector<std::unique_ptr<SpaceOptItem>> getOpt() {
        return std::move(items_);
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

    std::vector<std::unique_ptr<SpaceOptItem>> getOpts() {
        return spaceOpts_->getOpt();
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

}   // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
