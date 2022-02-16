/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/QueryEngine.h"

#include <gflags/gflags.h>  // for DECLARE_bool, DECL...

#include <atomic>       // for atomic_bool
#include <type_traits>  // for remove_reference<>...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/StatusOr.h"                  // for StatusOr
#include "common/charset/Charset.h"                // for CharsetInfo
#include "common/memory/MemoryUtils.h"             // for MemoryUtils, Memor...
#include "common/meta/ServerBasedIndexManager.h"   // for ServerBasedSchemaM...
#include "common/meta/ServerBasedSchemaManager.h"  // for ServerBasedSchemaM...
#include "graph/optimizer/OptRule.h"               // for RuleSet
#include "graph/planner/PlannersRegister.h"        // for PlannersRegister
#include "graph/service/QueryInstance.h"           // for QueryInstance

namespace nebula {
namespace meta {
class MetaClient;
}  // namespace meta
}  // namespace nebula

namespace folly {
class IOThreadPoolExecutor;

class IOThreadPoolExecutor;
}  // namespace folly
namespace nebula {
namespace meta {
class MetaClient;
}  // namespace meta
}  // namespace nebula

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
  storage_ = std::make_unique<storage::StorageClient>(ioExecutor, metaClient_);
  charsetInfo_ = CharsetInfo::instance();

  PlannersRegister::registerPlanners();

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

  auto ms = FLAGS_check_memory_interval_in_secs * 1000;
  memoryMonitorThread_->addRepeatTask(ms, updateMemoryWatermark);

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
