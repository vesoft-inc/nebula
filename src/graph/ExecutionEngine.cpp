/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ExecutionEngine.h"
#include "graph/ExecutionContext.h"
#include "graph/ExecutionPlan.h"
#include "storage/client/StorageClient.h"

DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {

ExecutionEngine::ExecutionEngine() {
}


ExecutionEngine::~ExecutionEngine() {
}


Status ExecutionEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    stats_ = std::make_unique<GraphStats>();

    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     HostAddr(0, 0),
                                                     0,
                                                     false,
                                                     stats_->getMetaClientStats());
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init(metaClient_.get());

    gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_.get());

    storage_ = std::make_unique<storage::StorageClient>(ioExecutor,
                                                        metaClient_.get(),
                                                        stats_->getStorageClientStats());
    return Status::OK();
}

void ExecutionEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<ExecutionContext>(std::move(rctx),
                                                   schemaManager_.get(),
                                                   gflagsManager_.get(),
                                                   storage_.get(),
                                                   metaClient_.get(),
                                                   stats_.get());
    // TODO(dutor) add support to plan cache
    auto plan = new ExecutionPlan(std::move(ectx));

    plan->execute();
}

}   // namespace graph
}   // namespace nebula
