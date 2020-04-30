/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/Executor.h"

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>

#include "exec/ExecutionError.h"
#include "exec/query/AggregateExecutor.h"
#include "exec/query/DedupExecutor.h"
#include "exec/query/FilterExecutor.h"
#include "exec/query/GetEdgesExecutor.h"
#include "exec/query/GetNeighborsExecutor.h"
#include "exec/query/GetVerticesExecutor.h"
#include "exec/query/IntersectExecutor.h"
#include "exec/query/LimitExecutor.h"
#include "exec/query/LoopExecutor.h"
#include "exec/query/MinusExecutor.h"
#include "exec/query/MultiOutputsExecutor.h"
#include "exec/query/ProjectExecutor.h"
#include "exec/query/ReadIndexExecutor.h"
#include "exec/query/SelectExecutor.h"
#include "exec/query/SortExecutor.h"
#include "exec/query/StartExecutor.h"
#include "exec/query/UnionExecutor.h"
#include "exec/query/SwitchSpaceExecutor.h"
#include "exec/admin/CreateSpaceExecutor.h"
#include "exec/admin/DescSpaceExecutor.h"
#include "exec/maintain/CreateTagExecutor.h"
#include "exec/maintain/DescTagExecutor.h"
#include "exec/maintain/CreateEdgeExecutor.h"
#include "exec/maintain/DescEdgeExecutor.h"
#include "exec/mutate/InsertVerticesExecutor.h"
#include "exec/mutate/InsertEdgesExecutor.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "planner/Admin.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "service/ExecutionContext.h"
#include "util/ObjectPool.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

Executor::Callable::Callable(const Executor *e) : planId(e->node()->id()) {
    DCHECK_NOTNULL(e);
}

// static
Executor *Executor::makeExecutor(const PlanNode *node,
                                 ExecutionContext *ectx,
                                 std::unordered_map<int64_t, Executor *> *cache) {
    auto iter = cache->find(node->id());
    if (iter != cache->end()) {
        return iter->second;
    }

    Executor *exec = nullptr;

    switch (node->kind()) {
        case PlanNode::Kind::kMultiOutputs: {
            auto mout = asNode<MultiOutputsNode>(node);
            auto input = makeExecutor(mout->input(), ectx, cache);
            exec = new MultiOutputsExecutor(mout, ectx, input);
            break;
        }
        case PlanNode::Kind::kAggregate: {
            auto agg = asNode<Aggregate>(node);
            auto input = makeExecutor(agg->input(), ectx, cache);
            exec = new AggregateExecutor(agg, ectx, input);
            break;
        }
        case PlanNode::Kind::kSort: {
            auto sort = asNode<Sort>(node);
            auto input = makeExecutor(sort->input(), ectx, cache);
            exec = new SortExecutor(sort, ectx, input);
            break;
        }
        case PlanNode::Kind::kFilter: {
            auto filter = asNode<Filter>(node);
            auto input = makeExecutor(filter->input(), ectx, cache);
            exec = new FilterExecutor(filter, ectx, input);
            break;
        }
        case PlanNode::Kind::kGetEdges: {
            auto ge = asNode<GetEdges>(node);
            auto input = makeExecutor(ge->input(), ectx, cache);
            exec = new GetEdgesExecutor(ge, ectx, input);
            break;
        }
        case PlanNode::Kind::kGetVertices: {
            auto gv = asNode<GetVertices>(node);
            auto input = makeExecutor(gv->input(), ectx, cache);
            exec = new GetVerticesExecutor(gv, ectx, input);
            break;
        }
        case PlanNode::Kind::kGetNeighbors: {
            auto gn = asNode<GetNeighbors>(node);
            auto input = makeExecutor(gn->input(), ectx, cache);
            exec = new GetNeighborsExecutor(gn, ectx, input);
            break;
        }
        case PlanNode::Kind::kLimit: {
            auto limit = asNode<Limit>(node);
            auto input = makeExecutor(limit->input(), ectx, cache);
            exec = new LimitExecutor(limit, ectx, input);
            break;
        }
        case PlanNode::Kind::kProject: {
            auto project = asNode<Project>(node);
            auto input = makeExecutor(project->input(), ectx, cache);
            exec = new ProjectExecutor(project, ectx, input);
            break;
        }
        case PlanNode::Kind::kReadIndex: {
            auto readIndex = asNode<ReadIndex>(node);
            auto input = makeExecutor(readIndex->input(), ectx, cache);
            exec = new ReadIndexExecutor(readIndex, ectx, input);
            break;
        }
        case PlanNode::Kind::kStart: {
            exec = new StartExecutor(node, ectx);
            break;
        }
        case PlanNode::Kind::kUnion: {
            auto uni = asNode<Union>(node);
            auto left = makeExecutor(uni->left(), ectx, cache);
            auto right = makeExecutor(uni->right(), ectx, cache);
            exec = new UnionExecutor(uni, ectx, left, right);
            break;
        }
        case PlanNode::Kind::kIntersect: {
            auto intersect = asNode<Intersect>(node);
            auto left = makeExecutor(intersect->left(), ectx, cache);
            auto right = makeExecutor(intersect->right(), ectx, cache);
            exec = new IntersectExecutor(intersect, ectx, left, right);
            break;
        }
        case PlanNode::Kind::kMinus: {
            auto minus = asNode<Minus>(node);
            auto left = makeExecutor(minus->left(), ectx, cache);
            auto right = makeExecutor(minus->right(), ectx, cache);
            exec = new MinusExecutor(minus, ectx, left, right);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = asNode<Loop>(node);
            auto input = makeExecutor(loop->input(), ectx, cache);
            auto body = makeExecutor(loop->body(), ectx, cache);
            exec = new LoopExecutor(loop, ectx, input, body);
            break;
        }
        case PlanNode::Kind::kSelector: {
            auto select = asNode<Selector>(node);
            auto input = makeExecutor(select->input(), ectx, cache);
            auto then = makeExecutor(select->then(), ectx, cache);
            auto els = makeExecutor(select->otherwise(), ectx, cache);
            exec = new SelectExecutor(select, ectx, input, then, els);
            break;
        }
        case PlanNode::Kind::kDedup: {
            auto dedup = asNode<Dedup>(node);
            auto input = makeExecutor(dedup->input(), ectx, cache);
            exec = new DedupExecutor(dedup, ectx, input);
            break;
        }
        case PlanNode::Kind::kSwitchSpace: {
            auto switchSpace = asNode<SwitchSpace>(node);
            auto input = makeExecutor(switchSpace->input(), ectx, cache);
            exec = new SwitchSpaceExecutor(switchSpace, ectx, input);
            break;
        }
        case PlanNode::Kind::kCreateSpace: {
            auto createSpace = asNode<CreateSpace>(node);
            exec = new CreateSpaceExecutor(createSpace, ectx);
            break;
        }
        case PlanNode::Kind::kDescSpace: {
            auto descSpace = asNode<DescSpace>(node);
            exec = new DescSpaceExecutor(descSpace, ectx);
            break;
        }
        case PlanNode::Kind::kCreateTag: {
            auto createTag = asNode<CreateTag>(node);
            exec = new CreateTagExecutor(createTag, ectx);
            break;
        }
        case PlanNode::Kind::kDescTag: {
            auto descTag = asNode<DescTag>(node);
            exec = new DescTagExecutor(descTag, ectx);
            break;
        }
        case PlanNode::Kind::kCreateEdge: {
            auto createEdge = asNode<CreateEdge>(node);
            exec = new CreateEdgeExecutor(createEdge, ectx);
            break;
        }
        case PlanNode::Kind::kDescEdge: {
            auto descEdge = asNode<DescEdge>(node);
            exec = new DescEdgeExecutor(descEdge, ectx);
            break;
        }
        case PlanNode::Kind::kInsertVertices: {
            auto insertV = asNode<InsertVertices>(node);
            exec = new InsertVerticesExecutor(insertV, ectx);
            break;
        }
        case PlanNode::Kind::kInsertEdges: {
            auto insertE = asNode<InsertEdges>(node);
            exec = new InsertEdgesExecutor(insertE, ectx);
            break;
        }
        case PlanNode::Kind::kUnknown:
        default:
            LOG(FATAL) << "Unknown plan node kind.";
            break;
    }

    DCHECK_NOTNULL(exec);

    cache->insert({node->id(), exec});
    return ectx->objPool()->add(exec);
}

int64_t Executor::id() const {
    return node()->id();
}

Executor::Executor(const std::string &name, const PlanNode *node, ExecutionContext *ectx)
    : name_(name), node_(node), ectx_(ectx) {
    DCHECK_NOTNULL(node_);
    DCHECK_NOTNULL(ectx_);

    // Initialize the position in ExecutionContext for each executor before execution plan
    // starting to run. This will avoid lock something for thread safety in real execution
    ectx_->addValue(node->varName(), nebula::Value());
}

folly::Future<Status> Executor::start(Status status) const {
    return folly::makeFuture(std::move(status)).via(runner());
}

folly::Future<Status> Executor::error(Status status) const {
    return folly::makeFuture<Status>(ExecutionError(std::move(status))).via(runner());
}

Status Executor::finish(nebula::Value &&value) {
    ectx_->addValue(node()->varName(), std::move(value));
    return Status::OK();
}

void Executor::dumpLog() const {
    VLOG(4) << name() << "(" << id() << ")";
}

folly::Executor *Executor::runner() const {
    if (!ectx() || !ectx()->rctx() || !ectx()->rctx()->runner()) {
        // This is just for test
        return &folly::InlineExecutor::instance();
    }
    return ectx()->rctx()->runner();
}

folly::Future<Status> SingleInputExecutor::execute() {
    return input_->execute();
}

folly::Future<Status> MultiInputsExecutor::execute() {
    std::vector<folly::Future<Status>> futures;
    for (auto *in : inputs_) {
        futures.emplace_back(in->execute());
    }
    return folly::collect(futures).then(cb([](std::vector<Status> ss) {
        for (auto &s : ss) {
            if (!s.ok()) return s;
        }
        return Status::OK();
    }));
}

}   // namespace graph
}   // namespace nebula
