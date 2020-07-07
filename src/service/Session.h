/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_SESSION_H_
#define COMMON_SESSION_H_

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

class Session final {
public:
    static std::shared_ptr<Session> create(int64_t id);

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
        /**
         * Only have one user as GOD, the user name is "root".
         */
        return user() == "root";
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
    std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles_;
};

}  // namespace graph
}  // namespace nebula

#endif   // COMMON_SESSION_H_
