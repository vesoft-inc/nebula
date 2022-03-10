/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_SESSION_SESSIONMANAGER_H_
#define COMMON_SESSION_SESSIONMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftTypes.h"

/**
 * SessionManager manages the client sessions, e.g. create new, find existing
 * and drop expired.
 */

namespace nebula {

class SessionCount {
 private:
  std::atomic<int32_t> count_ = 1;

 public:
  int fetch_add(int step) {
    count_.fetch_add(step, std::memory_order_release);
    return count_;
  }
  int fetch_sub(int step) {
    count_.fetch_sub(step, std::memory_order_release);
    return count_;
  }
  int get() {
    return count_;
  }
};

template <class SessionType>
class SessionManager {
 public:
  SessionManager(meta::MetaClient* metaClient, const HostAddr& hostAddr) {
    metaClient_ = metaClient;
    myAddr_ = hostAddr;
    scavenger_ = std::make_unique<thread::GenericWorker>();
    auto ok = scavenger_->start("session-manager");
    DCHECK(ok);
  }

  virtual ~SessionManager() {
    if (scavenger_ != nullptr) {
      scavenger_->stop();
      scavenger_->wait();
      scavenger_.reset();
    }
  }

  /**
   * Create a new session
   */
  virtual folly::Future<StatusOr<std::shared_ptr<SessionType>>> createSession(
      const std::string userName, const std::string clientIp, folly::Executor* runner) = 0;

  /**
   * Remove a session
   */
  virtual void removeSession(SessionID id) = 0;

  virtual folly::Future<StatusOr<std::shared_ptr<SessionType>>> findSession(
      SessionID id, folly::Executor* runner) = 0;

 protected:
  using SessionPtr = std::shared_ptr<SessionType>;
  using SessionCountPtr = std::shared_ptr<SessionCount>;
  folly::ConcurrentHashMap<SessionID, SessionPtr> activeSessions_;
  folly::ConcurrentHashMap<std::string, SessionCountPtr> userIpSessionCount_;
  std::unique_ptr<thread::GenericWorker> scavenger_;
  meta::MetaClient* metaClient_{nullptr};
  HostAddr myAddr_;
};

}  // namespace nebula

#endif  // COMMON_SESSION_SESSIONMANAGER_H_
