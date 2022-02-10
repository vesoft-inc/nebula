// Copyright (c) 2020 vesoft inc. All rights reserved.

// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_SESSION_CLIENTSESSION_H_
#define GRAPH_SESSION_CLIENTSESSION_H_

#include "clients/meta/MetaClient.h"
#include "common/time/Duration.h"
#include "interface/gen-cpp2/meta_types.h"

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

// A ClientSession is equivalent to a connection between a client and a server.
// ClientSession saves these information, including who created it, executed queries,
// space role, etc.
class ClientSession final {
 public:
  static std::shared_ptr<ClientSession> create(meta::cpp2::Session&& session,
                                               meta::MetaClient* metaClient);

  int64_t id() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_.get_session_id();
  }

  const SpaceInfo& space() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return space_;
  }

  void setSpace(SpaceInfo space) {
    {
      folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
      space_ = std::move(space);
      session_.space_name_ref() = space_.name;
    }
  }

  const std::string& spaceName() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_.get_space_name();
  }

  const std::string& user() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_.get_user_name();
  }

  const std::unordered_map<GraphSpaceID, meta::cpp2::RoleType>& roles() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return roles_;
  }

  StatusOr<meta::cpp2::RoleType> roleWithSpace(GraphSpaceID space) const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    auto ret = roles_.find(space);
    if (ret == roles_.end()) {
      return Status::Error("No role in space %d", space);
    }
    return ret->second;
  }

  // As long as a user has the GOD role in any space, the user must be a GOD user.
  bool isGod() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    // Cloud may have multiple God accounts
    for (auto& role : roles_) {
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

  // Reset the idle time of the session.
  void charge();

  int32_t getTimezone() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_.get_timezone();
  }

  const HostAddr& getGraphAddr() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_.get_graph_addr();
  }

  void setTimezone(int32_t timezone) {
    {
      folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
      session_.timezone_ref() = timezone;
      // TODO: if support ngql to set client's timezone,
      //  need to update the timezone config to metad when timezone executor
    }
  }

  void updateGraphAddr(const HostAddr& hostAddr) {
    {
      folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
      if (session_.get_graph_addr() == hostAddr) {
        return;
      }
      session_.graph_addr_ref() = hostAddr;
    }
  }

  meta::cpp2::Session getSession() const {
    folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
    return session_;
  }

  void updateSpaceName(const std::string& spaceName) {
    folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
    session_.space_name_ref() = spaceName;
  }

  void addQuery(QueryContext* qctx);

  void deleteQuery(QueryContext* qctx);

  bool findQuery(nebula::ExecutionPlanID epId) const;

  void markQueryKilled(nebula::ExecutionPlanID epId);

  void markAllQueryKilled();

 private:
  ClientSession() = default;

  explicit ClientSession(meta::cpp2::Session&& session, meta::MetaClient* metaClient);

 private:
  SpaceInfo space_;  // The space that the session is using.
  // When the idle time exceeds FLAGS_session_idle_timeout_secs,
  // the session will expire and then be reclaimed.
  time::Duration idleDuration_;
  meta::cpp2::Session session_;  // The session object used in RPC.
  meta::MetaClient* metaClient_{nullptr};
  mutable folly::RWSpinLock rwSpinLock_;

  // map<spaceId, role>
  // One user can have roles in multiple spaces
  // But a user has only one role in one space
  std::unordered_map<GraphSpaceID, meta::cpp2::RoleType> roles_;
  // An ExecutionPlanID represents a query.
  // A QueryContext also represents a query.
  std::unordered_map<ExecutionPlanID, QueryContext*> contexts_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_SESSION_CLIENTSESSION_H_
