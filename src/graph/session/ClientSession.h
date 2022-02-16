/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_SESSION_CLIENTSESSION_H_
#define GRAPH_SESSION_CLIENTSESSION_H_

#include <folly/synchronization/RWSpinLock.h>  // for RWSpinLock, RWSpinLock...
#include <stdint.h>                            // for int64_t, int32_t, uint...
#include <thrift/lib/cpp2/FieldRef.h>          // for field_ref

#include <memory>         // for shared_ptr
#include <string>         // for basic_string, string
#include <unordered_map>  // for unordered_map, _Node_c...
#include <utility>        // for move, pair

#include "clients/meta/MetaClient.h"
#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/thrift/ThriftTypes.h"      // for GraphSpaceID, Executio...
#include "common/time/Duration.h"           // for Duration
#include "interface/gen-cpp2/meta_types.h"  // for RoleType, Session, Spa...

namespace nebula {
namespace meta {
class MetaClient;

class MetaClient;
}  // namespace meta

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

  bool findQuery(::nebula::ExecutionPlanID epId) const;

  void markQueryKilled(::nebula::ExecutionPlanID epId);

  void markAllQueryKilled();

 private:
  ClientSession() = default;

  explicit ClientSession(meta::cpp2::Session&& session, meta::MetaClient* metaClient);

 private:
  SpaceInfo space_;
  time::Duration idleDuration_;
  meta::cpp2::Session session_;
  meta::MetaClient* metaClient_{nullptr};
  mutable folly::RWSpinLock rwSpinLock_;
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

#endif  // GRAPH_SESSION_CLIENTSESSION_H_
