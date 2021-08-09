/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef SESSION_CLIENTSESSION_H_
#define SESSION_CLIENTSESSION_H_

#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/time/Duration.h"

namespace nebula {
namespace graph {

class QueryContext;

constexpr int64_t kInvalidSpaceID = -1;
constexpr int64_t kInvalidSessionID = 0;

struct SpaceInfo {
    std::string name;
    GraphSpaceID id = kInvalidSpaceID;
    meta::cpp2::SpaceDesc spaceDesc;
};

class ClientSession final {
public:
    static std::shared_ptr<ClientSession> create(meta::cpp2::Session &&session,
                                                 meta::MetaClient* metaClient);

    int64_t id() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_.get_session_id();
    }

    const SpaceInfo space() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return space_;
    }

    void setSpace(SpaceInfo space) {
        {
            folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
            space_ = std::move(space);
            session_.set_space_name(space_.name);
        }
    }

    const std::string spaceName() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_.get_space_name();
    }

    const std::string user() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_.get_user_name();
    }

    std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return roles_;
    }

    StatusOr<meta::cpp2::RoleType> roleWithSpace(GraphSpaceID space) {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        auto ret = roles_.find(space);
        if (ret == roles_.end()) {
            return Status::Error("No role in space %d", space);
        }
        return ret->second;
    }

    bool isGod() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        // Cloud may have multiple God accounts
        for (auto &role : roles_) {
            if (role.second == meta::cpp2::RoleType::GOD) {
                return true;
            }
        }
        return false;
    }

    void setRole(GraphSpaceID space, meta::cpp2::RoleType role) {
        folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
        roles_.emplace(space, role);
    }

    uint64_t idleSeconds();

    void charge();

    int32_t getTimezone() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_.get_timezone();
    }

    HostAddr getGraphAddr() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_.get_graph_addr();
    }

    void setTimezone(int32_t timezone) {
        {
            folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
            session_.set_timezone(timezone);
            // TODO: if support ngql to set client's timezone,
            //  need to update the timezone config to metad when timezone executor
        }
    }

    void updateGraphAddr(const HostAddr &hostAddr) {
        {
            folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
            if (session_.get_graph_addr() == hostAddr) {
                return;
            }
            session_.set_graph_addr(hostAddr);
        }
    }

    const meta::cpp2::Session getSession() {
        folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
        return session_;
    }

    void updateSpaceName(const std::string &spaceName) {
        folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
        session_.set_space_name(spaceName);
    }

    void addQuery(QueryContext* qctx);

    void deleteQuery(QueryContext* qctx);

    bool findQuery(nebula::ExecutionPlanID epId);

    void markQueryKilled(nebula::ExecutionPlanID epId);

    void markAllQueryKilled();

private:
    ClientSession() = default;

    explicit ClientSession(meta::cpp2::Session &&session, meta::MetaClient* metaClient);

private:
    SpaceInfo               space_;
    time::Duration          idleDuration_;
    meta::cpp2::Session     session_;
    meta::MetaClient*       metaClient_{ nullptr};
    folly::RWSpinLock       rwSpinLock_;
    /*
     * map<spaceId, role>
     * One user can have roles in multiple spaces
     * But a user has only one role in one space
     */
    std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles_;
    std::unordered_map<ExecutionPlanID, QueryContext*> contexts_;
};

}  // namespace graph
}  // namespace nebula

#endif   // SESSION_CLIENTSESSION_H_
