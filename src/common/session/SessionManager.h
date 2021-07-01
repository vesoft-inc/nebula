/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_SESSION_SESSIONMANAGER_H_
#define COMMON_SESSION_SESSIONMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/clients/meta/MetaClient.h"


/**
 * SessionManager manages the client sessions, e.g. create new, find existing and drop expired.
 */

namespace nebula {

template <class SessionType>
class SessionManager {
public:
    SessionManager(meta::MetaClient* metaClient, const HostAddr &hostAddr) {
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
    virtual folly::Future<StatusOr<std::shared_ptr<SessionType>>>
    createSession(const std::string userName,
                  const std::string clientIp,
                  folly::Executor* runner) = 0;

    /**
     * Remove a session
     */
    virtual void removeSession(SessionID id) = 0;

    virtual folly::Future<StatusOr<std::shared_ptr<SessionType>>>
    findSession(SessionID id, folly::Executor* runner) = 0;

protected:
    using SessionPtr = std::shared_ptr<SessionType>;
    folly::ConcurrentHashMap<SessionID, SessionPtr> activeSessions_;
    std::unique_ptr<thread::GenericWorker>          scavenger_;
    meta::MetaClient                               *metaClient_{nullptr};
    HostAddr                                        myAddr_;
};

}   // namespace nebula


#endif  // COMMON_SESSION_SESSIONMANAGER_H_
