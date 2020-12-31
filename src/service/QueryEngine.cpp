/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/QueryEngine.h"

#include "common/base/Base.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "context/QueryContext.h"
#include "optimizer/OptRule.h"
#include "planner/PlannersRegister.h"
#include "service/QueryInstance.h"
#include "service/GraphFlags.h"
#include "version/Version.h"

DECLARE_bool(local_config);
DECLARE_bool(enable_optimizer);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {

Status QueryEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    meta::MetaClientOptions options;
    options.serviceName_ = "graph";
    options.skipConfig_ = FLAGS_local_config;
    options.role_ = meta::cpp2::HostRole::GRAPH;
    std::string localIP = network::NetworkUtils::getIPv4FromDevice(FLAGS_listen_netdev).value();
    options.localHost_ = HostAddr{localIP, FLAGS_port};
    options.gitInfoSHA_ = nebula::graph::gitInfoSha();
    metaClient_ =
        std::make_unique<meta::MetaClient>(ioExecutor, std::move(addrs.value()), options);
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    schemaManager_ = meta::ServerBasedSchemaManager::create(metaClient_.get());
    indexManager_ = meta::ServerBasedIndexManager::create(metaClient_.get());

    // gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_.get());

    storage_ = std::make_unique<storage::GraphStorageClient>(ioExecutor,
                                                             metaClient_.get());
    charsetInfo_ = CharsetInfo::instance();

    PlannersRegister::registPlanners();

    std::vector<const opt::RuleSet*> rulesets{&opt::RuleSet::DefaultRules()};
    if (FLAGS_enable_optimizer) {
        rulesets.emplace_back(&opt::RuleSet::QueryRules());
    }
    optimizer_ = std::make_unique<opt::Optimizer>(rulesets);

    return Status::OK();
}

void QueryEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<QueryContext>(std::move(rctx),
                                               schemaManager_.get(),
                                               indexManager_.get(),
                                               storage_.get(),
                                               metaClient_.get(),
                                               charsetInfo_);
    auto* instance = new QueryInstance(std::move(ectx), optimizer_.get());
    instance->execute();
}

}   // namespace graph
}   // namespace nebula
