/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_ADMINSENTENCES_H_
#define PARSER_ADMINSENTENCES_H_

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
        kShowSpaces
    };

    explicit ShowSentence(ShowType sType) {
        kind_ = Kind::kShow;
        showType_ = std::move(sType);
    }

    std::string toString() const override;

    ShowType showType() const {
        return showType_;
    }

private:
    ShowType    showType_{ShowType::kUnknown};
};


inline std::ostream& operator<<(std::ostream &os, ShowSentence::ShowType type) {
    return os << static_cast<uint32_t>(type);
}


class HostList final {
public:
    void addHost(std::string *hoststr) {
        hostStrs_.emplace_back(hoststr);
    }

    std::string toString() const;

    std::vector<HostAddr> toHosts() const {
        std::vector<HostAddr> result;
        result.resize(hostStrs_.size());
        auto getHostAddr = [] (const auto &ptr) {
            return network::NetworkUtils::toHostAddr(folly::trimWhitespace(*(ptr.get())));
        };
        std::transform(hostStrs_.begin(), hostStrs_.end(), result.begin(), getHostAddr);
        return result;
    }

private:
    std::vector<std::unique_ptr<std::string>>   hostStrs_;
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
        return hosts_->toHosts();
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
        return hosts_->toHosts();
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

    std::string* spaceName() {
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

    std::string* spaceName() {
        return spaceName_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>     spaceName_;
};

}   // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
