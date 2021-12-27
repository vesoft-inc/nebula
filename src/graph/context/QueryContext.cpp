/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

QueryContext::QueryContext(RequestContextPtr rctx,
                           meta::SchemaManager* sm,
                           meta::IndexManager* im,
                           storage::StorageClient* storage,
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
  // copy parameterMap into ExecutionContext
  if (rctx_) {
    for (auto item : rctx_->parameterMap()) {
      ectx_->setValue(std::move(item.first), std::move(item.second));
    }
  }
  idGen_ = std::make_unique<IdGenerator>(0);
  symTable_ = std::make_unique<SymbolTable>(objPool_.get());
  vctx_ = std::make_unique<ValidateContext>(std::make_unique<AnonVarGenerator>(symTable_.get()));
}

}  // namespace graph
}  // namespace nebula
