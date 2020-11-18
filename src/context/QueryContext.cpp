/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/QueryContext.h"

#include "common/interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

QueryContext::QueryContext(RequestContextPtr rctx,
                           meta::SchemaManager* sm,
                           meta::IndexManager* im,
                           storage::GraphStorageClient* storage,
                           meta::MetaClient* metaClient,
                           CharsetInfo* charsetInfo)
    : rctx_(std::move(rctx)),
      sm_(DCHECK_NOTNULL(sm)),
      im_(DCHECK_NOTNULL(im)),
      storageClient_(DCHECK_NOTNULL(storage)),
      metaClient_(DCHECK_NOTNULL(metaClient)),
      charsetInfo_(DCHECK_NOTNULL(charsetInfo)) {
    init();
}

QueryContext::QueryContext() {
    init();
}

void QueryContext::init() {
    objPool_ = std::make_unique<ObjectPool>();
    ep_ = std::make_unique<ExecutionPlan>();
    ectx_ = std::make_unique<ExecutionContext>();
    idGen_ = std::make_unique<IdGenerator>(0);
    symTable_ = std::make_unique<SymbolTable>(objPool_.get());
    vctx_ = std::make_unique<ValidateContext>(std::make_unique<AnonVarGenerator>(symTable_.get()));
}

void QueryContext::addProfilingData(int64_t planNodeId, ProfilingStats&& profilingStats) {
    // return directly if not enable profile
    if (!planDescription_) return;

    auto found = planDescription_->nodeIndexMap.find(planNodeId);
    DCHECK(found != planDescription_->nodeIndexMap.end());
    auto idx = found->second;
    auto& planNodeDesc = planDescription_->planNodeDescs[idx];
    if (planNodeDesc.profiles == nullptr) {
        planNodeDesc.profiles.reset(new std::vector<ProfilingStats>());
    }
    planNodeDesc.profiles->emplace_back(std::move(profilingStats));
}

void QueryContext::fillPlanDescription() {
    DCHECK(ep_ != nullptr);
    ep_->fillPlanDescription(planDescription_.get());
}

}   // namespace graph
}   // namespace nebula
