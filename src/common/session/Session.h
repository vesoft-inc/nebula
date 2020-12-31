/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_SESSION_H_
#define COMMON_SESSION_H_

#include "base/Base.h"
#include "time/Duration.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace graph {
class GlobalVariableHolder;
}
namespace session {

enum class Role : char {
    GOD           = 1,
    ADMIN         = 2,
    DBA           = 3,
    USER          = 4,
    GUEST         = 5,
    INVALID_ROLE  = 6,
};

class Session final {
public:
    static std::shared_ptr<Session> create(int64_t id);
    ~Session();

    int64_t id() const {
        return id_;
    }

    void setId(int64_t id) {
        id_ = id;
    }

    GraphSpaceID space() const {
        return space_;
    }

    void setSpace(const std::string &name, GraphSpaceID space) {
        spaceName_ = name;
        space_ = space;
    }

    const std::string& spaceName() const {
        return spaceName_;
    }

    const std::string& user() const {
        return account_;
    }

    void setAccount(std::string account) {
        account_ = std::move(account);
    }

    std::unordered_map<GraphSpaceID, Role> roles() const {
        return roles_;
    }

    Role roleWithSpace(GraphSpaceID space) const {
        auto ret = roles_.find(space);
        if (ret == roles_.end()) {
            return Role::INVALID_ROLE;
        }
        return ret->second;
    }

    Role toRole(nebula::cpp2::RoleType role) {
        switch (role) {
            case nebula::cpp2::RoleType::GOD : {
                return Role::GOD;
            }
            case nebula::cpp2::RoleType::ADMIN : {
                return Role::ADMIN;
            }
            case nebula::cpp2::RoleType::DBA : {
                return Role::DBA;
            }
            case nebula::cpp2::RoleType::USER : {
                return Role::USER;
            }
            case nebula::cpp2::RoleType::GUEST : {
                return Role::GUEST;
            }
        }
        return Role::INVALID_ROLE;
    }

    bool isGod() const {
        // Cloud may have multiple God accounts
        for (auto &role : roles_) {
            if (role.second == Role::GOD) {
                return true;
            }
        }
        return false;
    }

    void setRole(GraphSpaceID space, Role role) {
        roles_.emplace(space, role);
    }

    uint64_t idleSeconds() const;

    void charge();

    void setGlobalVariableHolder(std::unique_ptr<graph::GlobalVariableHolder> holder);

    graph::GlobalVariableHolder* globalVariableHolder();

private:
    Session() = default;
    explicit Session(int64_t id);


private:
    int64_t           id_{0};
    GraphSpaceID      space_{-1};
    std::string       spaceName_;
    std::string       account_;
    time::Duration    idleDuration_;
    /*
     * map<spaceId, role>
     * One user can have roles in multiple spaces
     * But a user has only one role in one space
     */
    std::unordered_map<GraphSpaceID, Role> roles_;
    std::unique_ptr<graph::GlobalVariableHolder> holder_;
};

}  // namespace session
}  // namespace nebula

#endif   // COMMON_SESSION_H_
