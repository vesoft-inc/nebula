/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/FilterExecutor.h"

#include "planner/Query.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
    dumpLog();
    auto* filter = asNode<Filter>(node());
    auto iter = ectx_->getResult(filter->inputVar()).iter();

    if (iter == nullptr) {
        LOG(ERROR) << "Internal Error: iterator is nullptr";
        return Status::Error("Internal Error: iterator is nullptr");
    }
    ResultBuilder builder;
    builder.value(iter->valuePtr());
    QueryExpressionContext ctx(ectx_, iter.get());
    auto condition = filter->condition();
    while (iter->valid()) {
        auto val = condition->eval(ctx);
        if (!val.isBool() && !val.isNull()) {
            return Status::Error("Internal Error: Wrong type result, "
                                 "should be NULL type or BOOL type");
        }
        if (val.isNull() || !val.getBool()) {
            iter->erase();
        } else {
            iter->next();
        }
    }

    iter->reset();
    builder.iter(std::move(iter));
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
