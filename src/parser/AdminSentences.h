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


class CreateSpaceSentence final : public Sentence {
public:
    CreateSpaceSentence(std::string *spaceName, int64_t partNum, int64_t replicaFactor) {
        spaceName_.reset(spaceName);
        partNum_ = static_cast<int32_t>(partNum);
        replicaFactor_ = static_cast<int32_t>(replicaFactor);
        kind_ = Kind::kCreateSpace;
    }

    std::string* spaceName() {
        return spaceName_.get();
    }

    int32_t partNum() {
        return partNum_;
    }

    int32_t replicaFactor() {
        return replicaFactor_;
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string> spaceName_;
    int32_t                      partNum_{0};
    int32_t                      replicaFactor_{0};
};
}   // namespace nebula

#endif  // PARSER_ADMINSENTENCES_H_
