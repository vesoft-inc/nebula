/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_SESSION_H_
#define COMMON_SESSION_H_

#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/time/Duration.h"

namespace nebula {
namespace graph {

constexpr int64_t kInvalidSpaceID = -1;
constexpr int64_t kInvalidSessionID = 0;

struct SpaceInfo {
    std::string name;
    GraphSpaceID id = kInvalidSpaceID;
    meta::cpp2::SpaceDesc spaceDesc;
};

class Session final {
public:
    static std::shared_ptr<Session> create(int64_t id);

    int64_t id() const {
        return id_;
    }

    void setId(int64_t id) {
        id_ = id;
    }

    const SpaceInfo& space() const {
        return space_;
    }

    void setSpace(SpaceInfo space) {
        space_ = std::move(space);
    }

    const std::string& user() const {
        return account_;
    }

    void setAccount(std::string account) {
        account_ = std::move(account);
    }

    std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles() const {
        return roles_;
    }

    StatusOr<meta::cpp2::RoleType> roleWithSpace(GraphSpaceID space) const {
        auto ret = roles_.find(space);
        if (ret == roles_.end()) {
            return Status::Error("No role in space %d", space);
        }
        return ret->second;
    }

    bool isGod() const {
        // Cloud may have multiple God accounts
        for (auto &role : roles_) {
            if (role.second == meta::cpp2::RoleType::GOD) {
                return true;
            }
        }
        return false;
    }

    void setRole(GraphSpaceID space, meta::cpp2::RoleType role) {
        roles_.emplace(space, role);
    }

    uint64_t idleSeconds() const;

    void charge();

private:
    Session() = default;
    explicit Session(int64_t id);


private:
    int64_t           id_{kInvalidSessionID};
    SpaceInfo         space_;
    std::string       account_;
    time::Duration    idleDuration_;
    /*
     * map<spaceId, role>
     * One user can have roles in multiple spaces
     * But a user has only one role in one space
     */
    std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles_;
};

}  // namespace graph
}  // namespace nebula

#endif   // COMMON_SESSION_H_
