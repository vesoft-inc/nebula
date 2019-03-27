/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_ADMINSENTENCES_H_
#define PARSER_ADMINSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"
#include "network/NetworkUtils.h"

namespace nebula {

using nebula::network::NetworkUtils;

enum class ShowKind : uint32_t {
    kUnknown,
    kShowHosts,
};


class ShowSentence final : public Sentence {
public:
    explicit ShowSentence(ShowKind sKind) {
        kind_ = Kind::kShow;
        showKind_ = std::move(sKind);
    }

    std::string toString() const override;

    ShowKind showKind() const {
        return showKind_;
    }

private:
    ShowKind    showKind_{ShowKind::kUnknown};
};

inline std::ostream& operator<<(std::ostream &os, ShowKind kind) {
    return os << static_cast<uint32_t>(kind);
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


class DeleteHostsSentence final : public Sentence {
public:
    AddHostsSentence() {
        kind_ = Kind::kDeleteHosts;
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
            LOG(FATAL) << "partition_num value illegal.";
        }
    }

    int64_t get_replica_factor() {
        if (isInt()) {
            return asInt();
        } else {
            LOG(FATAL) << "replica_factor value illegal.";
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
