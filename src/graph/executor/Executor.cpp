/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/Executor.h"

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>

#include <atomic>

#include "common/base/ObjectPool.h"
#include "common/memory/MemoryUtils.h"
#include "common/stats/StatsManager.h"
#include "common/time/ScopedTimer.h"
#include "graph/context/ExecutionContext.h"
#include "graph/context/QueryContext.h"
#include "graph/executor/ExecutionError.h"
#include "graph/executor/admin/AddHostsExecutor.h"
#include "graph/executor/admin/ChangePasswordExecutor.h"
#include "graph/executor/admin/CharsetExecutor.h"
#include "graph/executor/admin/ConfigExecutor.h"
#include "graph/executor/admin/CreateUserExecutor.h"
#include "graph/executor/admin/DescribeUserExecutor.h"
#include "graph/executor/admin/DownloadExecutor.h"
#include "graph/executor/admin/DropHostsExecutor.h"
#include "graph/executor/admin/DropUserExecutor.h"
#include "graph/executor/admin/GrantRoleExecutor.h"
#include "graph/executor/admin/IngestExecutor.h"
#include "graph/executor/admin/KillQueryExecutor.h"
#include "graph/executor/admin/ListRolesExecutor.h"
#include "graph/executor/admin/ListUserRolesExecutor.h"
#include "graph/executor/admin/ListUsersExecutor.h"
#include "graph/executor/admin/ListenerExecutor.h"
#include "graph/executor/admin/PartExecutor.h"
#include "graph/executor/admin/RevokeRoleExecutor.h"
#include "graph/executor/admin/SessionExecutor.h"
#include "graph/executor/admin/ShowHostsExecutor.h"
#include "graph/executor/admin/ShowMetaLeaderExecutor.h"
#include "graph/executor/admin/ShowQueriesExecutor.h"
#include "graph/executor/admin/ShowServiceClientsExecutor.h"
#include "graph/executor/admin/ShowStatsExecutor.h"
#include "graph/executor/admin/SignInServiceExecutor.h"
#include "graph/executor/admin/SignOutServiceExecutor.h"
#include "graph/executor/admin/SnapshotExecutor.h"
#include "graph/executor/admin/SpaceExecutor.h"
#include "graph/executor/admin/SubmitJobExecutor.h"
#include "graph/executor/admin/SwitchSpaceExecutor.h"
#include "graph/executor/admin/UpdateUserExecutor.h"
#include "graph/executor/admin/ZoneExecutor.h"
#include "graph/executor/algo/BFSShortestPathExecutor.h"
#include "graph/executor/algo/CartesianProductExecutor.h"
#include "graph/executor/algo/ConjunctPathExecutor.h"
#include "graph/executor/algo/ProduceAllPathsExecutor.h"
#include "graph/executor/algo/ProduceSemiShortestPathExecutor.h"
#include "graph/executor/algo/SubgraphExecutor.h"
#include "graph/executor/logic/ArgumentExecutor.h"
#include "graph/executor/logic/LoopExecutor.h"
#include "graph/executor/logic/PassThroughExecutor.h"
#include "graph/executor/logic/SelectExecutor.h"
#include "graph/executor/logic/StartExecutor.h"
#include "graph/executor/maintain/EdgeExecutor.h"
#include "graph/executor/maintain/EdgeIndexExecutor.h"
#include "graph/executor/maintain/FTIndexExecutor.h"
#include "graph/executor/maintain/TagExecutor.h"
#include "graph/executor/maintain/TagIndexExecutor.h"
#include "graph/executor/mutate/DeleteExecutor.h"
#include "graph/executor/mutate/InsertExecutor.h"
#include "graph/executor/mutate/UpdateExecutor.h"
#include "graph/executor/query/AggregateExecutor.h"
#include "graph/executor/query/AppendVerticesExecutor.h"
#include "graph/executor/query/AssignExecutor.h"
#include "graph/executor/query/DataCollectExecutor.h"
#include "graph/executor/query/DedupExecutor.h"
#include "graph/executor/query/FilterExecutor.h"
#include "graph/executor/query/GetEdgesExecutor.h"
#include "graph/executor/query/GetNeighborsExecutor.h"
#include "graph/executor/query/GetVerticesExecutor.h"
#include "graph/executor/query/IndexScanExecutor.h"
#include "graph/executor/query/InnerJoinExecutor.h"
#include "graph/executor/query/IntersectExecutor.h"
#include "graph/executor/query/LeftJoinExecutor.h"
#include "graph/executor/query/LimitExecutor.h"
#include "graph/executor/query/MinusExecutor.h"
#include "graph/executor/query/ProjectExecutor.h"
#include "graph/executor/query/SampleExecutor.h"
#include "graph/executor/query/ScanEdgesExecutor.h"
#include "graph/executor/query/ScanVerticesExecutor.h"
#include "graph/executor/query/SortExecutor.h"
#include "graph/executor/query/TopNExecutor.h"
#include "graph/executor/query/TraverseExecutor.h"
#include "graph/executor/query/UnionAllVersionVarExecutor.h"
#include "graph/executor/query/UnionExecutor.h"
#include "graph/executor/query/UnwindExecutor.h"
#include "graph/planner/plan/Admin.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Maintain.h"
#include "graph/planner/plan/Mutate.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"
#include "graph/stats/GraphStats.h"
#include "interface/gen-cpp2/graph_types.h"

using folly::stringPrintf;

DEFINE_bool(enable_lifetime_optimize, true, "Does enable the lifetime optimize.");
DECLARE_double(system_memory_high_watermark_ratio);

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
  auto &spaceName = qctx->rctx() ? qctx->rctx()->session()->spaceName() : "";
  switch (node->kind()) {
    case PlanNode::Kind::kPassThrough: {
      return pool->add(new PassThroughExecutor(node, qctx));
    }
    case PlanNode::Kind::kAggregate: {
      stats::StatsManager::addValue(kNumAggregateExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumAggregateExecutors, {{"space", spaceName}}));
      }
      return pool->add(new AggregateExecutor(node, qctx));
    }
    case PlanNode::Kind::kSort: {
      stats::StatsManager::addValue(kNumSortExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumSortExecutors, {{"space", spaceName}}));
      }
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
    case PlanNode::Kind::kScanEdges: {
      return pool->add(new ScanEdgesExecutor(node, qctx));
    }
    case PlanNode::Kind::kScanVertices: {
      return pool->add(new ScanVerticesExecutor(node, qctx));
    }
    case PlanNode::Kind::kGetNeighbors: {
      return pool->add(new GetNeighborsExecutor(node, qctx));
    }
    case PlanNode::Kind::kLimit: {
      return pool->add(new LimitExecutor(node, qctx));
    }
    case PlanNode::Kind::kSample: {
      return pool->add(new SampleExecutor(node, qctx));
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
      stats::StatsManager::addValue(kNumIndexScanExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumIndexScanExecutors, {{"space", spaceName}}));
      }
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
    case PlanNode::Kind::kCreateSpaceAs: {
      return pool->add(new CreateSpaceAsExecutor(node, qctx));
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
    case PlanNode::Kind::kClearSpace: {
      return pool->add(new ClearSpaceExecutor(node, qctx));
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
    case PlanNode::Kind::kDeleteTags: {
      return pool->add(new DeleteTagsExecutor(node, qctx));
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
    case PlanNode::Kind::kDescribeUser: {
      return pool->add(new DescribeUserExecutor(node, qctx));
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
    case PlanNode::Kind::kShowMetaLeader: {
      return pool->add(new ShowMetaLeaderExecutor(node, qctx));
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
    case PlanNode::Kind::kAddHosts: {
      return pool->add(new AddHostsExecutor(node, qctx));
    }
    case PlanNode::Kind::kDropHosts: {
      return pool->add(new DropHostsExecutor(node, qctx));
    }
    case PlanNode::Kind::kMergeZone: {
      return pool->add(new MergeZoneExecutor(node, qctx));
    }
    case PlanNode::Kind::kRenameZone: {
      return pool->add(new RenameZoneExecutor(node, qctx));
    }
    case PlanNode::Kind::kDropZone: {
      return pool->add(new DropZoneExecutor(node, qctx));
    }
    case PlanNode::Kind::kDivideZone: {
      return pool->add(new DivideZoneExecutor(node, qctx));
    }
    case PlanNode::Kind::kDescribeZone: {
      return pool->add(new DescribeZoneExecutor(node, qctx));
    }
    case PlanNode::Kind::kAddHostsIntoZone: {
      return pool->add(new AddHostsIntoZoneExecutor(node, qctx));
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
    case PlanNode::Kind::kShowServiceClients: {
      return pool->add(new ShowServiceClientsExecutor(node, qctx));
    }
    case PlanNode::Kind::kShowFTIndexes: {
      return pool->add(new ShowFTIndexesExecutor(node, qctx));
    }
    case PlanNode::Kind::kSignInService: {
      return pool->add(new SignInServiceExecutor(node, qctx));
    }
    case PlanNode::Kind::kSignOutService: {
      return pool->add(new SignOutServiceExecutor(node, qctx));
    }
    case PlanNode::Kind::kDownload: {
      return pool->add(new DownloadExecutor(node, qctx));
    }
    case PlanNode::Kind::kIngest: {
      return pool->add(new IngestExecutor(node, qctx));
    }
    case PlanNode::Kind::kShowSessions: {
      return pool->add(new ShowSessionsExecutor(node, qctx));
    }
    case PlanNode::Kind::kUpdateSession: {
      return pool->add(new UpdateSessionExecutor(node, qctx));
    }
    case PlanNode::Kind::kShowQueries: {
      return pool->add(new ShowQueriesExecutor(node, qctx));
    }
    case PlanNode::Kind::kKillQuery: {
      return pool->add(new KillQueryExecutor(node, qctx));
    }
    case PlanNode::Kind::kTraverse: {
      return pool->add(new TraverseExecutor(node, qctx));
    }
    case PlanNode::Kind::kAppendVertices: {
      return pool->add(new AppendVerticesExecutor(node, qctx));
    }
    case PlanNode::Kind::kBiLeftJoin: {
      return pool->add(new BiLeftJoinExecutor(node, qctx));
    }
    case PlanNode::Kind::kBiInnerJoin: {
      return pool->add(new BiInnerJoinExecutor(node, qctx));
    }
    case PlanNode::Kind::kBiCartesianProduct: {
      return pool->add(new BiCartesianProductExecutor(node, qctx));
    }
    case PlanNode::Kind::kArgument: {
      return pool->add(new ArgumentExecutor(node, qctx));
    }
    case PlanNode::Kind::kAlterSpace: {
      return pool->add(new AlterSpaceExecutor(node, qctx));
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
  // Initialize the position in ExecutionContext for each executor before
  // execution plan starting to run. This will avoid lock something for thread
  // safety in real execution
  if (!ectx_->exist(node->outputVar())) {
    ectx_->initVar(node->outputVar());
  }
}

Executor::~Executor() {}

Status Executor::open() {
  if (qctx_->isKilled()) {
    VLOG(1) << "Execution is being killed. session: " << qctx()->rctx()->session()->id()
            << "ep: " << qctx()->plan()->id() << "query: " << qctx()->rctx()->query();
    return Status::Error("Execution had been killed");
  }

  NG_RETURN_IF_ERROR(checkMemoryWatermark());

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

Status Executor::checkMemoryWatermark() {
  if (node_->isQueryNode() && MemoryUtils::kHitMemoryHighWatermark.load()) {
    stats::StatsManager::addValue(kNumQueriesHitMemoryWatermark);
    auto &spaceName = qctx()->rctx() ? qctx()->rctx()->session()->spaceName() : "";
    if (FLAGS_enable_space_level_metrics && spaceName != "") {
      stats::StatsManager::addValue(stats::StatsManager::counterWithLabels(
          kNumQueriesHitMemoryWatermark, {{"space", spaceName}}));
    }
    return Status::Error("Used memory hits the high watermark(%lf) of total system memory.",
                         FLAGS_system_memory_high_watermark_ratio);
  }
  return Status::OK();
}

folly::Future<Status> Executor::start(Status status) const {
  return folly::makeFuture(std::move(status)).via(runner());
}

folly::Future<Status> Executor::error(Status status) const {
  return folly::makeFuture<Status>(std::move(status)).via(runner());
}

void Executor::drop() {
  if (node()->kind() == PlanNode::Kind::kLoop) {
    // Release body when loop exit
    const auto *loopExecutor = static_cast<const LoopExecutor *>(this);
    const auto *loop = static_cast<const Loop *>(node());
    if (loop->loopLayers() != 1) {
      // Not the root Loop
      return;
    }
    if (loopExecutor->finally()) {
      dropBody(loop->body());
    }
    return;
  }
  if (node()->loopLayers() != 0) {
    // The lifetime of loop body is managed by Loop node
    return;
  }

  if (node()->kind() == PlanNode::Kind::kSelect) {
    // Release the branch don't execute
    const auto *selectExecutor = static_cast<const SelectExecutor *>(this);
    const auto *select = static_cast<const Select *>(node());
    if (selectExecutor->condition()) {
      dropBody(select->otherwise());
    } else {
      dropBody(select->then());
    }
    return;
  }
  // Normal node
  drop(node());
}

void Executor::drop(const PlanNode *node) {
  for (const auto &inputVar : node->inputVars()) {
    if (inputVar != nullptr) {
      // Make sure use the variable happened-before decrement count
      if (inputVar->userCount.fetch_sub(1, std::memory_order_release) == 1) {
        // Make sure drop happened-after count decrement
        CHECK_EQ(inputVar->userCount.load(std::memory_order_acquire), 0);
        ectx_->dropResult(inputVar->name);
        VLOG(1) << node->kind() << " Drop variable " << inputVar->name;
      }
    }
  }
}

void Executor::dropBody(const PlanNode *body) {
  drop(body);
  if (body->kind() == PlanNode::Kind::kSelect) {
    const auto *select = static_cast<const Select *>(body);
    dropBody(select->then());
    dropBody(select->otherwise());
  } else if (body->kind() == PlanNode::Kind::kLoop) {
    const auto *loop = static_cast<const Loop *>(body);
    dropBody(loop->body());
  }
  for (const auto &dep : body->dependencies()) {
    dropBody(dep);
  }
}

Status Executor::finish(Result &&result) {
  if (!FLAGS_enable_lifetime_optimize ||
      node()->outputVarPtr()->userCount.load(std::memory_order_relaxed) != 0) {
    numRows_ = !result.iterRef()->isGetNeighborsIter() ? result.size() : 0;
    result.checkMemory(node()->isQueryNode());
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
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).build());
}

folly::Executor *Executor::runner() const {
  if (!qctx() || !qctx()->rctx() || !qctx()->rctx()->runner()) {
    // This is just for test
    return &folly::InlineExecutor::instance();
  }
  return qctx()->rctx()->runner();
}

}  // namespace graph
}  // namespace nebula
