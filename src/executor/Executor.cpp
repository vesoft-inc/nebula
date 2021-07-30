/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/Executor.h"

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>
#include <atomic>

#include "common/base/Memory.h"
#include "common/base/ObjectPool.h"
#include "common/interface/gen-cpp2/graph_types.h"
#include "context/ExecutionContext.h"
#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/admin/BalanceExecutor.h"
#include "executor/admin/BalanceLeadersExecutor.h"
#include "executor/admin/ChangePasswordExecutor.h"
#include "executor/admin/CharsetExecutor.h"
#include "executor/admin/ConfigExecutor.h"
#include "executor/admin/CreateUserExecutor.h"
#include "executor/admin/DownloadExecutor.h"
#include "executor/admin/DropUserExecutor.h"
#include "executor/admin/GrantRoleExecutor.h"
#include "executor/admin/GroupExecutor.h"
#include "executor/admin/IngestExecutor.h"
#include "executor/admin/ListRolesExecutor.h"
#include "executor/admin/ListUserRolesExecutor.h"
#include "executor/admin/ListUsersExecutor.h"
#include "executor/admin/ListenerExecutor.h"
#include "executor/admin/PartExecutor.h"
#include "executor/admin/ResetBalanceExecutor.h"
#include "executor/admin/RevokeRoleExecutor.h"
#include "executor/admin/ShowBalanceExecutor.h"
#include "executor/admin/ShowHostsExecutor.h"
#include "executor/admin/ShowStatsExecutor.h"
#include "executor/admin/ShowTSClientsExecutor.h"
#include "executor/admin/SignInTSServiceExecutor.h"
#include "executor/admin/SignOutTSServiceExecutor.h"
#include "executor/admin/SnapshotExecutor.h"
#include "executor/admin/SpaceExecutor.h"
#include "executor/admin/StopBalanceExecutor.h"
#include "executor/admin/SubmitJobExecutor.h"
#include "executor/admin/SwitchSpaceExecutor.h"
#include "executor/admin/UpdateUserExecutor.h"
#include "executor/admin/ZoneExecutor.h"
#include "executor/admin/ShowQueriesExecutor.h"
#include "executor/admin/KillQueryExecutor.h"
#include "executor/algo/BFSShortestPathExecutor.h"
#include "executor/algo/CartesianProductExecutor.h"
#include "executor/algo/ConjunctPathExecutor.h"
#include "executor/algo/ProduceAllPathsExecutor.h"
#include "executor/algo/ProduceSemiShortestPathExecutor.h"
#include "executor/algo/SubgraphExecutor.h"
#include "executor/admin/SessionExecutor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "executor/logic/StartExecutor.h"
#include "executor/maintain/EdgeExecutor.h"
#include "executor/maintain/EdgeIndexExecutor.h"
#include "executor/maintain/TagExecutor.h"
#include "executor/maintain/TagIndexExecutor.h"
#include "executor/maintain/FTIndexExecutor.h"
#include "executor/mutate/DeleteExecutor.h"
#include "executor/mutate/InsertExecutor.h"
#include "executor/mutate/UpdateExecutor.h"
#include "executor/query/AggregateExecutor.h"
#include "executor/query/AssignExecutor.h"
#include "executor/query/DataCollectExecutor.h"
#include "executor/query/DedupExecutor.h"
#include "executor/query/FilterExecutor.h"
#include "executor/query/GetEdgesExecutor.h"
#include "executor/query/GetNeighborsExecutor.h"
#include "executor/query/GetVerticesExecutor.h"
#include "executor/query/IndexScanExecutor.h"
#include "executor/query/InnerJoinExecutor.h"
#include "executor/query/IntersectExecutor.h"
#include "executor/query/LeftJoinExecutor.h"
#include "executor/query/LimitExecutor.h"
#include "executor/query/MinusExecutor.h"
#include "executor/query/ProjectExecutor.h"
#include "executor/query/SortExecutor.h"
#include "executor/query/TopNExecutor.h"
#include "executor/query/UnionAllVersionVarExecutor.h"
#include "executor/query/UnionExecutor.h"
#include "executor/query/UnwindExecutor.h"
#include "planner/plan/Admin.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Maintain.h"
#include "planner/plan/Mutate.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "service/GraphFlags.h"
#include "util/ScopedTimer.h"

using folly::stringPrintf;

DEFINE_bool(enable_lifetime_optimize, true, "Does enable the lifetime optimize.");

namespace nebula {
namespace graph {

// static
Executor *Executor::create(const PlanNode *node, QueryContext *qctx) {
    std::unordered_map<int64_t, Executor *> visited;
    return makeExecutor(node, qctx, &visited);
}

// static
Executor *Executor::makeExecutor(const PlanNode *node,
                                 QueryContext *qctx,
                                 std::unordered_map<int64_t, Executor *> *visited) {
    DCHECK(qctx != nullptr);
    DCHECK(node != nullptr);
    auto iter = visited->find(node->id());
    if (iter != visited->end()) {
        return iter->second;
    }

    Executor *exec = makeExecutor(qctx, node);

    if (node->kind() == PlanNode::Kind::kSelect) {
        auto select = asNode<Select>(node);
        auto thenBody = makeExecutor(select->then(), qctx, visited);
        auto elseBody = makeExecutor(select->otherwise(), qctx, visited);
        auto selectExecutor = static_cast<SelectExecutor *>(exec);
        selectExecutor->setThenBody(thenBody);
        selectExecutor->setElseBody(elseBody);
    } else if (node->kind() == PlanNode::Kind::kLoop) {
        auto loop = asNode<Loop>(node);
        auto body = makeExecutor(loop->body(), qctx, visited);
        auto loopExecutor = static_cast<LoopExecutor *>(exec);
        loopExecutor->setLoopBody(body);
    }

    for (size_t i = 0; i < node->numDeps(); ++i) {
        exec->dependsOn(makeExecutor(node->dep(i), qctx, visited));
    }

    visited->insert({node->id(), exec});
    return exec;
}

// static
Executor *Executor::makeExecutor(QueryContext *qctx, const PlanNode *node) {
    auto pool = qctx->objPool();
    switch (node->kind()) {
        case PlanNode::Kind::kPassThrough: {
            return pool->add(new PassThroughExecutor(node, qctx));
        }
        case PlanNode::Kind::kAggregate: {
            return pool->add(new AggregateExecutor(node, qctx));
        }
        case PlanNode::Kind::kSort: {
            return pool->add(new SortExecutor(node, qctx));
        }
        case PlanNode::Kind::kTopN: {
            return pool->add(new TopNExecutor(node, qctx));
        }
        case PlanNode::Kind::kFilter: {
            return pool->add(new FilterExecutor(node, qctx));
        }
        case PlanNode::Kind::kGetEdges: {
            return pool->add(new GetEdgesExecutor(node, qctx));
        }
        case PlanNode::Kind::kGetVertices: {
            return pool->add(new GetVerticesExecutor(node, qctx));
        }
        case PlanNode::Kind::kGetNeighbors: {
            return pool->add(new GetNeighborsExecutor(node, qctx));
        }
        case PlanNode::Kind::kLimit: {
            return pool->add(new LimitExecutor(node, qctx));
        }
        case PlanNode::Kind::kProject: {
            return pool->add(new ProjectExecutor(node, qctx));
        }
        case PlanNode::Kind::kUnwind: {
            return pool->add(new UnwindExecutor(node, qctx));
        }
        case PlanNode::Kind::kIndexScan:
        case PlanNode::Kind::kEdgeIndexFullScan:
        case PlanNode::Kind::kEdgeIndexPrefixScan:
        case PlanNode::Kind::kEdgeIndexRangeScan:
        case PlanNode::Kind::kTagIndexFullScan:
        case PlanNode::Kind::kTagIndexPrefixScan:
        case PlanNode::Kind::kTagIndexRangeScan: {
            return pool->add(new IndexScanExecutor(node, qctx));
        }
        case PlanNode::Kind::kStart: {
            return pool->add(new StartExecutor(node, qctx));
        }
        case PlanNode::Kind::kUnion: {
            return pool->add(new UnionExecutor(node, qctx));
        }
        case PlanNode::Kind::kUnionAllVersionVar: {
            return pool->add(new UnionAllVersionVarExecutor(node, qctx));
        }
        case PlanNode::Kind::kIntersect: {
            return pool->add(new IntersectExecutor(node, qctx));
        }
        case PlanNode::Kind::kMinus: {
            return pool->add(new MinusExecutor(node, qctx));
        }
        case PlanNode::Kind::kLoop: {
            return pool->add(new LoopExecutor(node, qctx));
        }
        case PlanNode::Kind::kSelect: {
            return pool->add(new SelectExecutor(node, qctx));
        }
        case PlanNode::Kind::kDedup: {
            return pool->add(new DedupExecutor(node, qctx));
        }
        case PlanNode::Kind::kAssign: {
            return pool->add(new AssignExecutor(node, qctx));
        }
        case PlanNode::Kind::kSwitchSpace: {
            return pool->add(new SwitchSpaceExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateSpace: {
            return pool->add(new CreateSpaceExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescSpace: {
            return pool->add(new DescSpaceExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowSpaces: {
            return pool->add(new ShowSpacesExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropSpace: {
            return pool->add(new DropSpaceExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCreateSpace: {
            return pool->add(new ShowCreateSpaceExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateTag: {
            return pool->add(new CreateTagExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescTag: {
            return pool->add(new DescTagExecutor(node, qctx));
        }
        case PlanNode::Kind::kAlterTag: {
            return pool->add(new AlterTagExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateEdge: {
            return pool->add(new CreateEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescEdge: {
            return pool->add(new DescEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kAlterEdge: {
            return pool->add(new AlterEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowTags: {
            return pool->add(new ShowTagsExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowEdges: {
            return pool->add(new ShowEdgesExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropTag: {
            return pool->add(new DropTagExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropEdge: {
            return pool->add(new DropEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCreateTag: {
            return pool->add(new ShowCreateTagExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCreateEdge: {
            return pool->add(new ShowCreateEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateTagIndex: {
            return pool->add(new CreateTagIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateEdgeIndex: {
            return pool->add(new CreateEdgeIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateFTIndex: {
            return pool->add(new CreateFTIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropTagIndex: {
            return pool->add(new DropTagIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropEdgeIndex: {
            return pool->add(new DropEdgeIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropFTIndex: {
            return pool->add(new DropFTIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescTagIndex: {
            return pool->add(new DescTagIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescEdgeIndex: {
            return pool->add(new DescEdgeIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCreateTagIndex: {
            return pool->add(new ShowCreateTagIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCreateEdgeIndex: {
            return pool->add(new ShowCreateEdgeIndexExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowTagIndexes: {
            return pool->add(new ShowTagIndexesExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowEdgeIndexes: {
            return pool->add(new ShowEdgeIndexesExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowTagIndexStatus: {
            return pool->add(new ShowTagIndexStatusExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowEdgeIndexStatus: {
            return pool->add(new ShowEdgeIndexStatusExecutor(node, qctx));
        }
        case PlanNode::Kind::kInsertVertices: {
            return pool->add(new InsertVerticesExecutor(node, qctx));
        }
        case PlanNode::Kind::kInsertEdges: {
            return pool->add(new InsertEdgesExecutor(node, qctx));
        }
        case PlanNode::Kind::kDataCollect: {
            return pool->add(new DataCollectExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateSnapshot: {
            return pool->add(new CreateSnapshotExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropSnapshot: {
            return pool->add(new DropSnapshotExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowSnapshots: {
            return pool->add(new ShowSnapshotsExecutor(node, qctx));
        }
        case PlanNode::Kind::kLeftJoin: {
            return pool->add(new LeftJoinExecutor(node, qctx));
        }
        case PlanNode::Kind::kInnerJoin: {
            return pool->add(new InnerJoinExecutor(node, qctx));
        }
        case PlanNode::Kind::kDeleteVertices: {
            return pool->add(new DeleteVerticesExecutor(node, qctx));
        }
        case PlanNode::Kind::kDeleteEdges: {
            return pool->add(new DeleteEdgesExecutor(node, qctx));
        }
        case PlanNode::Kind::kUpdateVertex: {
            return pool->add(new UpdateVertexExecutor(node, qctx));
        }
        case PlanNode::Kind::kUpdateEdge: {
            return pool->add(new UpdateEdgeExecutor(node, qctx));
        }
        case PlanNode::Kind::kCreateUser: {
            return pool->add(new CreateUserExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropUser: {
            return pool->add(new DropUserExecutor(node, qctx));
        }
        case PlanNode::Kind::kUpdateUser: {
            return pool->add(new UpdateUserExecutor(node, qctx));
        }
        case PlanNode::Kind::kGrantRole: {
            return pool->add(new GrantRoleExecutor(node, qctx));
        }
        case PlanNode::Kind::kRevokeRole: {
            return pool->add(new RevokeRoleExecutor(node, qctx));
        }
        case PlanNode::Kind::kChangePassword: {
            return pool->add(new ChangePasswordExecutor(node, qctx));
        }
        case PlanNode::Kind::kListUserRoles: {
            return pool->add(new ListUserRolesExecutor(node, qctx));
        }
        case PlanNode::Kind::kListUsers: {
            return pool->add(new ListUsersExecutor(node, qctx));
        }
        case PlanNode::Kind::kListRoles: {
            return pool->add(new ListRolesExecutor(node, qctx));
        }
        case PlanNode::Kind::kBalanceLeaders: {
            return pool->add(new BalanceLeadersExecutor(node, qctx));
        }
        case PlanNode::Kind::kBalance: {
            return pool->add(new BalanceExecutor(node, qctx));
        }
        case PlanNode::Kind::kStopBalance: {
            return pool->add(new StopBalanceExecutor(node, qctx));
        }
        case PlanNode::Kind::kResetBalance: {
            return pool->add(new ResetBalanceExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowBalance: {
            return pool->add(new ShowBalanceExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowConfigs: {
            return pool->add(new ShowConfigsExecutor(node, qctx));
        }
        case PlanNode::Kind::kSetConfig: {
            return pool->add(new SetConfigExecutor(node, qctx));
        }
        case PlanNode::Kind::kGetConfig: {
            return pool->add(new GetConfigExecutor(node, qctx));
        }
        case PlanNode::Kind::kSubmitJob: {
            return pool->add(new SubmitJobExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowHosts: {
            return pool->add(new ShowHostsExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowParts: {
            return pool->add(new ShowPartsExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCharset: {
            return pool->add(new ShowCharsetExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowCollation: {
            return pool->add(new ShowCollationExecutor(node, qctx));
        }
        case PlanNode::Kind::kBFSShortest: {
            return pool->add(new BFSShortestPathExecutor(node, qctx));
        }
        case PlanNode::Kind::kProduceSemiShortestPath: {
            return pool->add(new ProduceSemiShortestPathExecutor(node, qctx));
        }
        case PlanNode::Kind::kConjunctPath: {
            return pool->add(new ConjunctPathExecutor(node, qctx));
        }
        case PlanNode::Kind::kProduceAllPaths: {
            return pool->add(new ProduceAllPathsExecutor(node, qctx));
        }
        case PlanNode::Kind::kCartesianProduct: {
            return pool->add(new CartesianProductExecutor(node, qctx));
        }
        case PlanNode::Kind::kSubgraph: {
            return pool->add(new SubgraphExecutor(node, qctx));
        }
        case PlanNode::Kind::kAddGroup: {
            return pool->add(new AddGroupExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropGroup: {
            return pool->add(new DropGroupExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescribeGroup: {
            return pool->add(new DescribeGroupExecutor(node, qctx));
        }
        case PlanNode::Kind::kAddZoneIntoGroup: {
            return pool->add(new AddZoneIntoGroupExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropZoneFromGroup: {
            return pool->add(new DropZoneFromGroupExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowGroups: {
            return pool->add(new ListGroupsExecutor(node, qctx));
        }
        case PlanNode::Kind::kAddZone: {
            return pool->add(new AddZoneExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropZone: {
            return pool->add(new DropZoneExecutor(node, qctx));
        }
        case PlanNode::Kind::kDescribeZone: {
            return pool->add(new DescribeZoneExecutor(node, qctx));
        }
        case PlanNode::Kind::kAddHostIntoZone: {
            return pool->add(new AddHostIntoZoneExecutor(node, qctx));
        }
        case PlanNode::Kind::kDropHostFromZone: {
            return pool->add(new DropHostFromZoneExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowZones: {
            return pool->add(new ListZonesExecutor(node, qctx));
        }
        case PlanNode::Kind::kAddListener: {
            return pool->add(new AddListenerExecutor(node, qctx));
        }
        case PlanNode::Kind::kRemoveListener: {
            return pool->add(new RemoveListenerExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowListener: {
            return pool->add(new ShowListenerExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowStats: {
            return pool->add(new ShowStatsExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowTSClients: {
            return pool->add(new ShowTSClientsExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowFTIndexes: {
            return pool->add(new ShowFTIndexesExecutor(node, qctx));
        }
        case PlanNode::Kind::kSignInTSService: {
            return pool->add(new SignInTSServiceExecutor(node, qctx));
        }
        case PlanNode::Kind::kSignOutTSService: {
            return pool->add(new SignOutTSServiceExecutor(node, qctx));
        }
        case PlanNode::Kind::kDownload: {
            return pool->add(new DownloadExecutor(node, qctx));
        }
        case PlanNode::Kind::kIngest: {
            return pool->add(new IngestExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowSessions:  {
            return pool->add(new ShowSessionsExecutor(node, qctx));
        }
        case PlanNode::Kind::kUpdateSession:  {
            return pool->add(new UpdateSessionExecutor(node, qctx));
        }
        case PlanNode::Kind::kShowQueries: {
            return pool->add(new ShowQueriesExecutor(node, qctx));
        }
        case PlanNode::Kind::kKillQuery: {
            return pool->add(new KillQueryExecutor(node, qctx));
        }
        case PlanNode::Kind::kUnknown: {
            LOG(FATAL) << "Unknown plan node kind " << static_cast<int32_t>(node->kind());
            break;
        }
    }
    return nullptr;
}

Executor::Executor(const std::string &name, const PlanNode *node, QueryContext *qctx)
    : id_(DCHECK_NOTNULL(node)->id()),
      name_(name),
      node_(DCHECK_NOTNULL(node)),
      qctx_(DCHECK_NOTNULL(qctx)),
      ectx_(DCHECK_NOTNULL(qctx->ectx())) {
    // Initialize the position in ExecutionContext for each executor before execution plan
    // starting to run. This will avoid lock something for thread safety in real execution
    if (!ectx_->exist(node->outputVar())) {
        ectx_->initVar(node->outputVar());
    }
}

Executor::~Executor() {}

Status Executor::open() {
    if (qctx_->isKilled()) {
        VLOG(1) << "Execution is being killed. session: " << qctx()->rctx()->session()->id()
            << "ep: " << qctx()->plan()->id()
            << "query: " << qctx()->rctx()->query();
        return Status::Error("Execution had been killed");
    }
    auto status = MemInfo::make();
    NG_RETURN_IF_ERROR(status);
    auto mem = std::move(status).value();
    if (node_->isQueryNode() && mem->hitsHighWatermark(FLAGS_system_memory_high_watermark_ratio)) {
        return Status::Error(
            "Used memory(%ldKB) hits the high watermark(%lf) of total system memory(%ldKB).",
            mem->usedInKB(),
            FLAGS_system_memory_high_watermark_ratio,
            mem->totalInKB());
    }
    numRows_ = 0;
    execTime_ = 0;
    totalDuration_.reset();
    return Status::OK();
}

Status Executor::close() {
    ProfilingStats stats;
    stats.totalDurationInUs = totalDuration_.elapsedInUSec();
    stats.rows = numRows_;
    stats.execDurationInUs = execTime_;
    if (!otherStats_.empty()) {
        stats.otherStats =
            std::make_unique<std::unordered_map<std::string, std::string>>(std::move(otherStats_));
    }
    qctx()->plan()->addProfileStats(node_->id(), std::move(stats));
    return Status::OK();
}

folly::Future<Status> Executor::start(Status status) const {
    return folly::makeFuture(std::move(status)).via(runner());
}

folly::Future<Status> Executor::error(Status status) const {
    return folly::makeFuture<Status>(std::move(status)).via(runner());
}

void Executor::drop() {
    for (const auto &inputVar : node()->inputVars()) {
        if (inputVar != nullptr) {
            // Make sure use the variable happened-before decrement count
            if (inputVar->userCount.fetch_sub(1, std::memory_order_release) == 1) {
                // Make sure drop happened-after count decrement
                CHECK_EQ(inputVar->userCount.load(std::memory_order_acquire), 0);
                ectx_->dropResult(inputVar->name);
                VLOG(1) << "Drop variable " << node()->outputVar();
            }
        }
    }
}

Status Executor::finish(Result &&result) {
    if (!FLAGS_enable_lifetime_optimize ||
        node()->outputVarPtr()->userCount.load(std::memory_order_relaxed) != 0) {
        numRows_ = result.size();
        ectx_->setResult(node()->outputVar(), std::move(result));
    } else {
        VLOG(1) << "Drop variable " << node()->outputVar();
    }
    if (FLAGS_enable_lifetime_optimize) {
        drop();
    }
    return Status::OK();
}

Status Executor::finish(Value &&value) {
    return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).finish());
}

folly::Executor *Executor::runner() const {
    if (!qctx() || !qctx()->rctx() || !qctx()->rctx()->runner()) {
        // This is just for test
        return &folly::InlineExecutor::instance();
    }
    return qctx()->rctx()->runner();
}

}   // namespace graph
}   // namespace nebula
