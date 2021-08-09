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

    return Status::OK();
}

void QueryEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<QueryContext>(std::move(rctx),
                                               schemaManager_.get(),
                                               indexManager_.get(),
                                               storage_.get(),
                                               metaClient_,
                                               charsetInfo_);
    auto* instance = new QueryInstance(std::move(ectx), optimizer_.get());
    instance->execute();
}

}   // namespace graph
}   // namespace nebula
