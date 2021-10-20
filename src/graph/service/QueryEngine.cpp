/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/service/QueryEngine.h"

#include "common/base/Base.h"
#include "common/memory/MemoryUtils.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/PlannersRegister.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/QueryInstance.h"
#include "version/Version.h"

DECLARE_bool(local_config);
DECLARE_bool(enable_optimizer);
DECLARE_string(meta_server_addrs);
DEFINE_int32(check_memory_interval_in_secs, 1, "Memory check interval in seconds");

namespace nebula {
namespace graph {

Status QueryEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                         meta::MetaClient* metaClient) {
  metaClient_ = metaClient;
  schemaManager_ = meta::ServerBasedSchemaManager::create(metaClient_);
  indexManager_ = meta::ServerBasedIndexManager::create(metaClient_);
  storage_ = std::make_unique<storage::GraphStorageClient>(ioExecutor, metaClient_);
  charsetInfo_ = CharsetInfo::instance();

  PlannersRegister::registPlanners();

  std::vector<const opt::RuleSet*> rulesets{&opt::RuleSet::DefaultRules()};
  if (FLAGS_enable_optimizer) {
    rulesets.emplace_back(&opt::RuleSet::QueryRules());
  }
  optimizer_ = std::make_unique<opt::Optimizer>(rulesets);

  return setupMemoryMonitorThread();
}

void QueryEngine::execute(RequestContextPtr rctx) {
  auto qctx = std::make_unique<QueryContext>(std::move(rctx),
                                             schemaManager_.get(),
                                             indexManager_.get(),
                                             storage_.get(),
                                             metaClient_,
                                             charsetInfo_);
  auto* instance = new QueryInstance(std::move(qctx), optimizer_.get());
  instance->execute();
}

Status QueryEngine::setupMemoryMonitorThread() {
  memoryMonitorThread_ = std::make_unique<thread::GenericWorker>();
  if (!memoryMonitorThread_ || !memoryMonitorThread_->start("graph-memory-monitor")) {
    return Status::Error("Fail to start query engine background thread.");
  }

  auto updateMemoryWatermark = []() -> Status {
    auto status = MemoryUtils::hitsHighWatermark();
    NG_RETURN_IF_ERROR(status);
    MemoryUtils::kHitMemoryHighWatermark.store(std::move(status).value());
    return Status::OK();
  };

  // Just to test whether to get the right memory info
  NG_RETURN_IF_ERROR(updateMemoryWatermark());

  memoryMonitorThread_->addRepeatTask(FLAGS_check_memory_interval_in_secs, updateMemoryWatermark);

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
