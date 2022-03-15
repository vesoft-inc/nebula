// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/session/ClientSession.h"

#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"
#include "graph/context/QueryContext.h"
#include "graph/stats/GraphStats.h"

namespace nebula {
namespace graph {

ClientSession::ClientSession(meta::cpp2::Session&& session, meta::MetaClient* metaClient) {
  session_ = std::move(session);
  metaClient_ = metaClient;
}

std::shared_ptr<ClientSession> ClientSession::create(meta::cpp2::Session&& session,
                                                     meta::MetaClient* metaClient) {
  return std::shared_ptr<ClientSession>(new ClientSession(std::move(session), metaClient));
}

void ClientSession::charge() {
  folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
  idleDuration_.reset();
  session_.update_time_ref() = time::WallClock::fastNowInMicroSec();
}

uint64_t ClientSession::idleSeconds() {
  folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
  return idleDuration_.elapsedInSec();
}

void ClientSession::addQuery(QueryContext* qctx) {
  auto epId = qctx->plan()->id();
  meta::cpp2::QueryDesc queryDesc;
  queryDesc.start_time_ref() = time::WallClock::fastNowInMicroSec();
  queryDesc.status_ref() = meta::cpp2::QueryStatus::RUNNING;
  queryDesc.query_ref() = qctx->rctx()->query();
  queryDesc.graph_addr_ref() = session_.get_graph_addr();
  VLOG(1) << "Add query: " << qctx->rctx()->query() << ", epId: " << epId;

  folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
  contexts_.emplace(epId, qctx);
  session_.queries_ref()->emplace(epId, std::move(queryDesc));
}

void ClientSession::deleteQuery(QueryContext* qctx) {
  auto epId = qctx->plan()->id();
  VLOG(1) << "Delete query, epId: " << epId;
  folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
  contexts_.erase(epId);
  session_.queries_ref()->erase(epId);
}

bool ClientSession::findQuery(nebula::ExecutionPlanID epId) const {
  folly::RWSpinLock::ReadHolder rHolder(rwSpinLock_);
  auto context = contexts_.find(epId);
  if (context != contexts_.end()) {
    return true;
  }

  auto query = session_.queries_ref()->find(epId);
  if (query != session_.queries_ref()->end()) {
    return true;
  }
  return false;
}

void ClientSession::markQueryKilled(nebula::ExecutionPlanID epId) {
  folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
  auto context = contexts_.find(epId);
  if (context == contexts_.end()) {
    return;
  }
  context->second->markKilled();
  stats::StatsManager::addValue(kNumKilledQueries);
  if (FLAGS_enable_space_level_metrics && space_.name != "") {
    stats::StatsManager::addValue(
        stats::StatsManager::counterWithLabels(kNumKilledQueries, {{"space", space_.name}}));
  }
  VLOG(1) << "Mark query killed in local cache, epId: " << epId;

  auto query = session_.queries_ref()->find(epId);
  if (query == session_.queries_ref()->end()) {
    return;
  }
  query->second.status_ref() = meta::cpp2::QueryStatus::KILLING;
  VLOG(1) << "Mark query killed in meta, epId: " << epId;
}

void ClientSession::markAllQueryKilled() {
  folly::RWSpinLock::WriteHolder wHolder(rwSpinLock_);
  for (auto& context : contexts_) {
    context.second->markKilled();
    session_.queries_ref()->clear();
  }
  stats::StatsManager::addValue(kNumKilledQueries, contexts_.size());
  if (FLAGS_enable_space_level_metrics && space_.name != "") {
    stats::StatsManager::addValue(
        stats::StatsManager::counterWithLabels(kNumKilledQueries, {{"space", space_.name}}),
        contexts_.size());
  }
}
}  // namespace graph
}  // namespace nebula
