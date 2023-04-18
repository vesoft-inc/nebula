// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/logic/ArgumentExecutor.h"

#include "graph/planner/plan/Logic.h"

namespace nebula {
namespace graph {

ArgumentExecutor::ArgumentExecutor(const PlanNode *node, QueryContext *qctx)
    : Executor("ArgumentExecutor", node, qctx) {}

folly::Future<Status> ArgumentExecutor::execute() {
  // MemoryTrackerVerified
  auto *argNode = asNode<Argument>(node());
  auto &alias = argNode->getAlias();
  auto iter = DCHECK_NOTNULL(ectx_->getResult(argNode->inputVar()).iter());

  const auto &successor = successors();
  auto kind = (*successor.begin())->node()->kind();

  auto sz = iter->size();

  DataSet ds;
  ds.colNames = argNode->colNames();
  ds.rows.reserve(sz);

  VidHashSet unique;
  unique.reserve(sz);

  auto addRow = [&unique, &ds](const Value &v) {
    if (unique.emplace(v).second) {
      Row row;
      row.values.emplace_back(v);
      ds.rows.emplace_back(std::move(row));
    }
  };

  for (; iter->valid(); iter->next()) {
    auto &val = iter->getColumn(alias);
    if (val.isNull()) {
      continue;
    }

    if (val.isVertex()) {
      addRow(val);
      continue;
    }

    switch (kind) {
      case PlanNode::Kind::kTraverse: {
        if (val.isList()) {
          for (auto &v : val.getList().values) {
            addRow(v);
          }
          break;
        }
        [[fallthrough]];
      }
      case PlanNode::Kind::kGetVertices:
      case PlanNode::Kind::kExpand: {
        // TODO(jmq) analyze the type of val in the validation phase
        addRow(val);
        break;
      }
      default: {
        return Status::Error("Argument only support vertex, but got %s, which type is '%s'",
                             val.toString().c_str(),
                             val.typeName().c_str());
      }
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
