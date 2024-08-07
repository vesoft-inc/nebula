/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/session/SessionManagerProcessor.h"

#include <folly/Random.h>

namespace nebula {
namespace meta {

void CreateSessionProcessor::process(const cpp2::CreateSessionReq &req) {
  auto user = req.get_user();
  cpp2::Session session;
  int64_t rand = 0;
  while (rand == 0) {
    rand = static_cast<int64_t>(folly::Random::rand64());
  }
  session.session_id_ref() = rand;  // 0 is used as a special value in kill query
  session.create_time_ref() = time::WallClock::fastNowInMicroSec();
  session.update_time_ref() = session.get_create_time();
  session.user_name_ref() = user;
  session.graph_addr_ref() = req.get_graph_addr();
  session.client_ip_ref() = req.get_client_ip();
  resp_.session_ref() = session;

  // AtomicOp for create
  auto getAtomicOp = [this, session = std::move(session), user = std::move(user)]() mutable {
    kvstore::MergeableAtomicOpResult atomicOp;
    // read userKey
    atomicOp.readSet.emplace_back(MetaKeyUtils::userKey(user));
    auto ret = userExist(user);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "User does not exist, errorCode: " << apache::thrift::util::enumNameSafe(ret);
      atomicOp.code = ret;
      return atomicOp;
    }
    auto batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto sessionKey = MetaKeyUtils::sessionKey(session.get_session_id());
    // write sessionKey
    atomicOp.writeSet.emplace_back(sessionKey);
    batchHolder->put(std::move(sessionKey), MetaKeyUtils::sessionVal(session));
    atomicOp.batch = encodeBatchValue(batchHolder->getBatch());
    atomicOp.code = nebula::cpp2::ErrorCode::SUCCEEDED;
    return atomicOp;
  };

  auto cb = [this](nebula::cpp2::ErrorCode ec) {
    if (ec != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Put data error on meta server, errorCode: "
                << apache::thrift::util::enumNameSafe(ec);
    }
    handleErrorCode(ec);
    onFinished();
  };

  kvstore_->asyncAtomicOp(kDefaultSpaceId, kDefaultPartId, std::move(getAtomicOp), std::move(cb));
}

void UpdateSessionsProcessor::process(const cpp2::UpdateSessionsReq &req) {
  // AtomicOp for update
  auto getAtomicOp = [this, req]() mutable {
    kvstore::MergeableAtomicOpResult atomicOp;
    auto batchHolder = std::make_unique<kvstore::BatchHolder>();
    std::unordered_map<nebula::SessionID,
                       std::unordered_map<nebula::ExecutionPlanID, cpp2::QueryDesc>>
        killedQueries;
    std::vector<SessionID> killedSessions;

    for (auto &session : req.get_sessions()) {
      auto sessionId = session.get_session_id();
      auto sessionKey = MetaKeyUtils::sessionKey(sessionId);
      atomicOp.readSet.emplace_back(sessionKey);
      auto ret = doGet(sessionKey);
      if (!nebula::ok(ret)) {
        auto errCode = nebula::error(ret);
        // If the session requested to be updated can not be found in meta, we consider the session
        // has been killed
        if (errCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
          killedSessions.emplace_back(sessionId);
          continue;
        } else {
          atomicOp.code = errCode;
          return atomicOp;
        }
      }

      // update sessions to be saved if query is being killed, and return them to
      // client.
      auto &newQueries = *session.queries_ref();
      std::unordered_map<nebula::ExecutionPlanID, cpp2::QueryDesc> killedQueriesInCurrentSession;
      auto sessionInMeta = MetaKeyUtils::parseSessionVal(nebula::value(ret));
      for (const auto &savedQuery : sessionInMeta.get_queries()) {
        auto epId = savedQuery.first;
        auto newQuery = newQueries.find(epId);
        if (newQuery == newQueries.end()) {
          continue;
        }
        auto &desc = savedQuery.second;
        if (desc.get_status() == cpp2::QueryStatus::KILLING) {
          const_cast<cpp2::QueryDesc &>(newQuery->second).status_ref() = cpp2::QueryStatus::KILLING;
          killedQueriesInCurrentSession.emplace(epId, desc);
        }
      }
      if (!killedQueriesInCurrentSession.empty()) {
        killedQueries[sessionId] = std::move(killedQueriesInCurrentSession);
      }

      if (sessionInMeta.get_update_time() > session.get_update_time()) {
        continue;
      }

      atomicOp.writeSet.emplace_back(sessionKey);
      batchHolder->put(std::move(sessionKey), MetaKeyUtils::sessionVal(session));
    }

    resp_.killed_queries_ref() = std::move(killedQueries);
    resp_.killed_sessions_ref() = std::move(killedSessions);

    atomicOp.batch = encodeBatchValue(batchHolder->getBatch());
    atomicOp.code = nebula::cpp2::ErrorCode::SUCCEEDED;
    return atomicOp;
  };

  auto cb = [this](nebula::cpp2::ErrorCode ec) {
    if (ec != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Put data error on meta server, errorCode: "
                << apache::thrift::util::enumNameSafe(ec);
    }
    handleErrorCode(ec);
    onFinished();
  };

  kvstore_->asyncAtomicOp(kDefaultSpaceId, kDefaultPartId, std::move(getAtomicOp), std::move(cb));
}

void ListSessionsProcessor::process(const cpp2::ListSessionsReq &) {
  auto &prefix = MetaKeyUtils::sessionPrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    handleErrorCode(nebula::error(ret));
    onFinished();
    return;
  }

  std::vector<cpp2::Session> sessions;
  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto session = MetaKeyUtils::parseSessionVal(iter->val());
    VLOG(3) << "List session: " << session.get_session_id();
    sessions.emplace_back(std::move(session));
    iter->next();
  }
  LOG(INFO) << "resp session size: " << sessions.size();
  resp_.sessions_ref() = std::move(sessions);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void GetSessionProcessor::process(const cpp2::GetSessionReq &req) {
  auto sessionId = req.get_session_id();
  auto sessionKey = MetaKeyUtils::sessionKey(sessionId);
  auto ret = doGet(sessionKey);
  if (!nebula::ok(ret)) {
    auto errCode = nebula::error(ret);
    LOG(INFO) << "Session id `" << sessionId << "' not found";
    if (errCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      errCode = nebula::cpp2::ErrorCode::E_SESSION_NOT_FOUND;
    }
    handleErrorCode(errCode);
    onFinished();
    return;
  }

  auto session = MetaKeyUtils::parseSessionVal(nebula::value(ret));
  resp_.session_ref() = std::move(session);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void RemoveSessionProcessor::process(const cpp2::RemoveSessionReq &req) {
  std::vector<SessionID> killedSessions;
  auto sessionIds = req.get_session_ids();

  for (auto sessionId : sessionIds) {
    // AtomicOp for remove
    auto getAtomicOp = [this, sessionId] {
      kvstore::MergeableAtomicOpResult atomicOp;
      auto batchHolder = std::make_unique<kvstore::BatchHolder>();
      auto sessionKey = MetaKeyUtils::sessionKey(sessionId);
      atomicOp.readSet.emplace_back(sessionKey);
      auto ret = doGet(sessionKey);
      if (!nebula::ok(ret)) {
        atomicOp.code = nebula::error(ret);
        return atomicOp;
      }
      atomicOp.writeSet.emplace_back(sessionKey);
      batchHolder->remove(std::move(sessionKey));
      atomicOp.batch = encodeBatchValue(batchHolder->getBatch());
      atomicOp.code = nebula::cpp2::ErrorCode::SUCCEEDED;
      return atomicOp;
    };

    // Remove session key from kvstore
    folly::Baton<true, std::atomic> baton;
    nebula::cpp2::ErrorCode errorCode;
    kvstore_->asyncAtomicOp(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(getAtomicOp),
                            [this, &baton, &errorCode](nebula::cpp2::ErrorCode code) {
                              this->handleErrorCode(code);
                              errorCode = code;
                              baton.post();
                            });
    baton.wait();

    // continue if the session is not removed successfully
    if (errorCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Remove session key failed, error code: " << static_cast<int32_t>(errorCode);
      if (errorCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        continue;
      } else {
        // for other error like leader change, we handle the error and return.
        handleErrorCode(errorCode);
        onFinished();
        return;
      }
    }

    // record the removed session id
    killedSessions.emplace_back(sessionId);
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.removed_session_ids_ref() = std::move(killedSessions);
  onFinished();
}

void KillQueryProcessor::process(const cpp2::KillQueryReq &req) {
  auto &killQueries = req.get_kill_queries();
  auto getAtomicOp = [this, killQueries] {
    kvstore::MergeableAtomicOpResult atomicOp;
    auto batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto &kv : killQueries) {
      auto sessionId = kv.first;
      auto sessionKey = MetaKeyUtils::sessionKey(sessionId);
      atomicOp.readSet.emplace_back(sessionKey);
      auto ret = doGet(sessionKey);
      if (!nebula::ok(ret)) {
        auto errCode = nebula::error(ret);
        if (errCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
          errCode = nebula::cpp2::ErrorCode::E_SESSION_NOT_FOUND;
        }
        atomicOp.code = errCode;
        return atomicOp;
      }

      auto session = MetaKeyUtils::parseSessionVal(nebula::value(ret));
      for (auto &epId : kv.second) {
        auto query = session.queries_ref()->find(epId);
        if (query == session.queries_ref()->end()) {
          atomicOp.code = nebula::cpp2::ErrorCode::E_QUERY_NOT_FOUND;
          return atomicOp;
        }
        query->second.status_ref() = cpp2::QueryStatus::KILLING;
      }

      atomicOp.writeSet.emplace_back(sessionKey);
      batchHolder->put(std::move(sessionKey), MetaKeyUtils::sessionVal(session));
    }

    atomicOp.batch = encodeBatchValue(batchHolder->getBatch());
    atomicOp.code = nebula::cpp2::ErrorCode::SUCCEEDED;
    return atomicOp;
  };

  auto cb = [this](nebula::cpp2::ErrorCode ec) {
    if (ec != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Put data error on meta server, errorCode: "
                << apache::thrift::util::enumNameSafe(ec);
    }
    handleErrorCode(ec);
    onFinished();
  };

  kvstore_->asyncAtomicOp(kDefaultSpaceId, kDefaultPartId, std::move(getAtomicOp), std::move(cb));
}

}  // namespace meta
}  // namespace nebula
