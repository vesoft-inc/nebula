/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/AppendVerticesExecutor.h"

namespace nebula {
namespace graph {
folly::Future<Status> AppendVerticesExecutor::execute() {
  // TODO
  auto *av = asNode<AppendVertices>(node());
  return finish(ResultBuilder().value(ectx_->getResult(av->inputVar()).valuePtr()).build());
}

DataSet AppendVerticesExecutor::buildRequestDataSet(const AppendVertices *av) {
  // TODO
  UNUSED(av);
  return DataSet();
}

folly::Future<Status> AppendVerticesExecutor::appendVertices() {
  // TODO
  return folly::makeFuture<Status>(Status::OK());
}
}  // namespace graph
}  // namespace nebula
