/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_REQUESTCONTEXT_H_
#define GRAPH_REQUESTCONTEXT_H_
#include <boost/core/noncopyable.hpp>

#include "common/base/Base.h"
#include "common/cpp/helpers.h"
#include "common/time/Duration.h"
#include "graph/session/ClientSession.h"
#include "graph/session/GraphSessionManager.h"
#include "interface/gen-cpp2/GraphService.h"

/**
 * RequestContext holds context infos of a specific request from a client.
 * The typical use is:
 *  1. Create a RequestContext, with statement, session, etc.
 *  2. Obtain a Future from the context, which is to be returned back to the
 * Thrift framework.
 *  3. Prepare the Response when the request is completed.
 *  4. Call `finish' to send the response to the client.
 */

namespace nebula {
namespace graph {

template <typename Response>
class RequestContext final : public boost::noncopyable, public cpp::NonMovable {
 public:
  RequestContext() = default;
  ~RequestContext() {
    if (session_ != nullptr) {
      // Keep the session active
      session_->charge();
    }
  }

  void setQuery(std::string query) {
    query_ = std::move(query);
  }

  const std::string& query() const {
    return query_;
  }

  Response& resp() {
    return resp_;
  }

  folly::Future<Response> future() {
    return promise_.getFuture();
  }

  void setSession(std::shared_ptr<ClientSession> session) {
    session_ = std::move(session);
    if (session_ != nullptr) {
      // Keep the session active
      session_->charge();
    }
  }

  ClientSession* session() const {
    return session_.get();
  }

  folly::Executor* runner() const {
    return runner_;
  }

  void setRunner(folly::Executor* runner) {
    runner_ = runner;
  }

  const time::Duration& duration() const {
    return duration_;
  }

  void finish() {
    promise_.setValue(std::move(resp_));
  }

  void setSessionMgr(GraphSessionManager* mgr) {
    sessionMgr_ = mgr;
  }

  GraphSessionManager* sessionMgr() const {
    return sessionMgr_;
  }

  void setParameterMap(std::unordered_map<std::string, Value> parameterMap) {
    parameterMap_ = std::move(parameterMap);
  }

  const std::unordered_map<std::string, Value>& parameterMap() const {
    return parameterMap_;
  }

 private:
  time::Duration duration_;
  std::string query_;
  Response resp_;
  folly::Promise<Response> promise_;
  std::shared_ptr<ClientSession> session_;
  folly::Executor* runner_{nullptr};
  GraphSessionManager* sessionMgr_{nullptr};
  std::unordered_map<std::string, Value> parameterMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_REQUESTCONTEXT_H_
