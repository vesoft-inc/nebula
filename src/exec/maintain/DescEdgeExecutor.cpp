/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/maintain/DescEdgeExecutor.h"
#include "planner/Maintain.h"
#include "service/ExecutionContext.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> DescEdgeExecutor::execute() {
    return descEdge().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> DescEdgeExecutor::descEdge() {
    dumpLog();

    auto *deNode = asNode<DescEdge>(node());
    return ectx()->getMetaClient()->getEdgeSchema(deNode->getSpaceId(), deNode->getName())
            .via(runner())
            .then([this](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toDescSchema(resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                finish(Value(std::move(ret).value()));
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
