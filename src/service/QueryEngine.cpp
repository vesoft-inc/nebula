/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "service/QueryEngine.h"
#include "service/QueryInstance.h"
#include "service/ExecutionContext.h"

DECLARE_bool(local_config);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {

QueryEngine::QueryEngine() {
}


QueryEngine::~QueryEngine() {
}


Status QueryEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    meta::MetaClientOptions options;
    options.serviceName_ = "graph";
    options.skipConfig_ = FLAGS_local_config;
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init(metaClient_.get());

    // gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_.get());

    storage_ = std::make_unique<storage::GraphStorageClient>(ioExecutor,
                                                        metaClient_.get());
    return Status::OK();
}

void QueryEngine::execute(RequestContextPtr rctx) {
    // TODO: Use QueryContext.
    auto ectx = std::make_unique<ExecutionContext>(std::move(rctx),
                                                   schemaManager_.get(),
                                                   storage_.get(),
                                                   metaClient_.get());
    auto* instance = new QueryInstance(std::move(ectx));
    instance->execute();
}

}   // namespace graph
}   // namespace nebula
