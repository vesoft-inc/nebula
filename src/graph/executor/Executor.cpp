// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/Executor.h"

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>

#include <algorithm>
#include <atomic>

#include "common/base/ObjectPool.h"
#include "common/memory/MemoryUtils.h"
#include "common/stats/StatsManager.h"
#include "graph/context/ExecutionContext.h"
#include "graph/executor/ExecutionError.h"
#include "graph/executor/admin/AddHostsExecutor.h"
#include "graph/executor/admin/ChangePasswordExecutor.h"
#include "graph/executor/admin/CharsetExecutor.h"
#include "graph/executor/admin/ConfigExecutor.h"
#include "graph/executor/admin/CreateUserExecutor.h"
#include "graph/executor/admin/DescribeUserExecutor.h"
#include "graph/executor/admin/DropHostsExecutor.h"
#include "graph/executor/admin/DropUserExecutor.h"
#include "graph/executor/admin/GrantRoleExecutor.h"
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
#include "graph/executor/algo/AllPathsExecutor.h"
#include "graph/executor/algo/BFSShortestPathExecutor.h"
#include "graph/executor/algo/CartesianProductExecutor.h"
#include "graph/executor/algo/MultiShortestPathExecutor.h"
#include "graph/executor/algo/ShortestPathExecutor.h"
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
#include "graph/executor/query/ExpandAllExecutor.h"
#include "graph/executor/query/ExpandExecutor.h"
#include "graph/executor/query/FilterExecutor.h"
#include "graph/executor/query/FulltextIndexScanExecutor.h"
#include "graph/executor/query/GetEdgesExecutor.h"
#include "graph/executor/query/GetNeighborsExecutor.h"
#include "graph/executor/query/GetVerticesExecutor.h"
#include "graph/executor/query/IndexScanExecutor.h"
#include "graph/executor/query/InnerJoinExecutor.h"
#include "graph/executor/query/IntersectExecutor.h"
#include "graph/executor/query/LeftJoinExecutor.h"
#include "graph/executor/query/LimitExecutor.h"
#include "graph/executor/query/MinusExecutor.h"
#include "graph/executor/query/PatternApplyExecutor.h"
#include "graph/executor/query/ProjectExecutor.h"
#include "graph/executor/query/RollUpApplyExecutor.h"
#include "graph/executor/query/SampleExecutor.h"
#include "graph/executor/query/ScanEdgesExecutor.h"
#include "graph/executor/query/ScanVerticesExecutor.h"
#include "graph/executor/query/SortExecutor.h"
#include "graph/executor/query/TopNExecutor.h"
#include "graph/executor/query/TraverseExecutor.h"
#include "graph/executor/query/UnionAllVersionVarExecutor.h"
#include "graph/executor/query/UnionExecutor.h"
#include "graph/executor/query/UnwindExecutor.h"
#include "graph/executor/query/ValueExecutor.h"
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
      return pool->makeAndAdd<PassThroughExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAggregate: {
      stats::StatsManager::addValue(kNumAggregateExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumAggregateExecutors, {{"space", spaceName}}));
      }
      return pool->makeAndAdd<AggregateExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSort: {
      stats::StatsManager::addValue(kNumSortExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumSortExecutors, {{"space", spaceName}}));
      }
      return pool->makeAndAdd<SortExecutor>(node, qctx);
    }
    case PlanNode::Kind::kTopN: {
      return pool->makeAndAdd<TopNExecutor>(node, qctx);
    }
    case PlanNode::Kind::kFilter: {
      return pool->makeAndAdd<FilterExecutor>(node, qctx);
    }
    case PlanNode::Kind::kGetEdges: {
      return pool->makeAndAdd<GetEdgesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kGetVertices: {
      return pool->makeAndAdd<GetVerticesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kScanEdges: {
      return pool->makeAndAdd<ScanEdgesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kScanVertices: {
      return pool->makeAndAdd<ScanVerticesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kGetNeighbors: {
      return pool->makeAndAdd<GetNeighborsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kExpand: {
      return pool->makeAndAdd<ExpandExecutor>(node, qctx);
    }
    case PlanNode::Kind::kExpandAll: {
      return pool->makeAndAdd<ExpandAllExecutor>(node, qctx);
    }
    case PlanNode::Kind::kFulltextIndexScan: {
      return pool->makeAndAdd<FulltextIndexScanExecutor>(node, qctx);
    }
    case PlanNode::Kind::kLimit: {
      stats::StatsManager::addValue(kNumLimitExecutors);
      if (FLAGS_enable_space_level_metrics && spaceName != "") {
        stats::StatsManager::addValue(
            stats::StatsManager::counterWithLabels(kNumLimitExecutors, {{"space", spaceName}}));
      }
      return pool->makeAndAdd<LimitExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSample: {
      return pool->makeAndAdd<SampleExecutor>(node, qctx);
    }
    case PlanNode::Kind::kProject: {
      return pool->makeAndAdd<ProjectExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUnwind: {
      return pool->makeAndAdd<UnwindExecutor>(node, qctx);
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
      return pool->makeAndAdd<IndexScanExecutor>(node, qctx);
    }
    case PlanNode::Kind::kStart: {
      return pool->makeAndAdd<StartExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUnion: {
      return pool->makeAndAdd<UnionExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUnionAllVersionVar: {
      return pool->makeAndAdd<UnionAllVersionVarExecutor>(node, qctx);
    }
    case PlanNode::Kind::kIntersect: {
      return pool->makeAndAdd<IntersectExecutor>(node, qctx);
    }
    case PlanNode::Kind::kMinus: {
      return pool->makeAndAdd<MinusExecutor>(node, qctx);
    }
    case PlanNode::Kind::kLoop: {
      return pool->makeAndAdd<LoopExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSelect: {
      return pool->makeAndAdd<SelectExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDedup: {
      return pool->makeAndAdd<DedupExecutor>(node, qctx);
    }
    case PlanNode::Kind::kValue: {
      return pool->makeAndAdd<ValueExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAssign: {
      return pool->makeAndAdd<AssignExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSwitchSpace: {
      return pool->makeAndAdd<SwitchSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateSpace: {
      return pool->makeAndAdd<CreateSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateSpaceAs: {
      return pool->makeAndAdd<CreateSpaceAsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescSpace: {
      return pool->makeAndAdd<DescSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowSpaces: {
      return pool->makeAndAdd<ShowSpacesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropSpace: {
      return pool->makeAndAdd<DropSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kClearSpace: {
      return pool->makeAndAdd<ClearSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCreateSpace: {
      return pool->makeAndAdd<ShowCreateSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateTag: {
      return pool->makeAndAdd<CreateTagExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescTag: {
      return pool->makeAndAdd<DescTagExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAlterTag: {
      return pool->makeAndAdd<AlterTagExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateEdge: {
      return pool->makeAndAdd<CreateEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescEdge: {
      return pool->makeAndAdd<DescEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAlterEdge: {
      return pool->makeAndAdd<AlterEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowTags: {
      return pool->makeAndAdd<ShowTagsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowEdges: {
      return pool->makeAndAdd<ShowEdgesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropTag: {
      return pool->makeAndAdd<DropTagExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropEdge: {
      return pool->makeAndAdd<DropEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCreateTag: {
      return pool->makeAndAdd<ShowCreateTagExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCreateEdge: {
      return pool->makeAndAdd<ShowCreateEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateTagIndex: {
      return pool->makeAndAdd<CreateTagIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateEdgeIndex: {
      return pool->makeAndAdd<CreateEdgeIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateFTIndex: {
      return pool->makeAndAdd<CreateFTIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropTagIndex: {
      return pool->makeAndAdd<DropTagIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropEdgeIndex: {
      return pool->makeAndAdd<DropEdgeIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropFTIndex: {
      return pool->makeAndAdd<DropFTIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescTagIndex: {
      return pool->makeAndAdd<DescTagIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescEdgeIndex: {
      return pool->makeAndAdd<DescEdgeIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCreateTagIndex: {
      return pool->makeAndAdd<ShowCreateTagIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCreateEdgeIndex: {
      return pool->makeAndAdd<ShowCreateEdgeIndexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowTagIndexes: {
      return pool->makeAndAdd<ShowTagIndexesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowEdgeIndexes: {
      return pool->makeAndAdd<ShowEdgeIndexesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowTagIndexStatus: {
      return pool->makeAndAdd<ShowTagIndexStatusExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowEdgeIndexStatus: {
      return pool->makeAndAdd<ShowEdgeIndexStatusExecutor>(node, qctx);
    }
    case PlanNode::Kind::kInsertVertices: {
      return pool->makeAndAdd<InsertVerticesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kInsertEdges: {
      return pool->makeAndAdd<InsertEdgesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDataCollect: {
      return pool->makeAndAdd<DataCollectExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateSnapshot: {
      return pool->makeAndAdd<CreateSnapshotExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropSnapshot: {
      return pool->makeAndAdd<DropSnapshotExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowSnapshots: {
      return pool->makeAndAdd<ShowSnapshotsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kInnerJoin: {
      return pool->makeAndAdd<InnerJoinExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDeleteVertices: {
      return pool->makeAndAdd<DeleteVerticesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDeleteTags: {
      return pool->makeAndAdd<DeleteTagsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDeleteEdges: {
      return pool->makeAndAdd<DeleteEdgesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUpdateVertex: {
      return pool->makeAndAdd<UpdateVertexExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUpdateEdge: {
      return pool->makeAndAdd<UpdateEdgeExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCreateUser: {
      return pool->makeAndAdd<CreateUserExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropUser: {
      return pool->makeAndAdd<DropUserExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUpdateUser: {
      return pool->makeAndAdd<UpdateUserExecutor>(node, qctx);
    }
    case PlanNode::Kind::kGrantRole: {
      return pool->makeAndAdd<GrantRoleExecutor>(node, qctx);
    }
    case PlanNode::Kind::kRevokeRole: {
      return pool->makeAndAdd<RevokeRoleExecutor>(node, qctx);
    }
    case PlanNode::Kind::kChangePassword: {
      return pool->makeAndAdd<ChangePasswordExecutor>(node, qctx);
    }
    case PlanNode::Kind::kListUserRoles: {
      return pool->makeAndAdd<ListUserRolesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kListUsers: {
      return pool->makeAndAdd<ListUsersExecutor>(node, qctx);
    }
    case PlanNode::Kind::kListRoles: {
      return pool->makeAndAdd<ListRolesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescribeUser: {
      return pool->makeAndAdd<DescribeUserExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowConfigs: {
      return pool->makeAndAdd<ShowConfigsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSetConfig: {
      return pool->makeAndAdd<SetConfigExecutor>(node, qctx);
    }
    case PlanNode::Kind::kGetConfig: {
      return pool->makeAndAdd<GetConfigExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSubmitJob: {
      return pool->makeAndAdd<SubmitJobExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowHosts: {
      return pool->makeAndAdd<ShowHostsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowMetaLeader: {
      return pool->makeAndAdd<ShowMetaLeaderExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowParts: {
      return pool->makeAndAdd<ShowPartsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCharset: {
      return pool->makeAndAdd<ShowCharsetExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowCollation: {
      return pool->makeAndAdd<ShowCollationExecutor>(node, qctx);
    }
    case PlanNode::Kind::kBFSShortest: {
      return pool->makeAndAdd<BFSShortestPathExecutor>(node, qctx);
    }
    case PlanNode::Kind::kMultiShortestPath: {
      return pool->makeAndAdd<MultiShortestPathExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAllPaths: {
      return pool->makeAndAdd<AllPathsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCartesianProduct: {
      return pool->makeAndAdd<CartesianProductExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSubgraph: {
      return pool->makeAndAdd<SubgraphExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAddHosts: {
      return pool->makeAndAdd<AddHostsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropHosts: {
      return pool->makeAndAdd<DropHostsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kMergeZone: {
      return pool->makeAndAdd<MergeZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kRenameZone: {
      return pool->makeAndAdd<RenameZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDropZone: {
      return pool->makeAndAdd<DropZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDivideZone: {
      return pool->makeAndAdd<DivideZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kDescribeZone: {
      return pool->makeAndAdd<DescribeZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAddHostsIntoZone: {
      return pool->makeAndAdd<AddHostsIntoZoneExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowZones: {
      return pool->makeAndAdd<ListZonesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAddListener: {
      return pool->makeAndAdd<AddListenerExecutor>(node, qctx);
    }
    case PlanNode::Kind::kRemoveListener: {
      return pool->makeAndAdd<RemoveListenerExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowListener: {
      return pool->makeAndAdd<ShowListenerExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowStats: {
      return pool->makeAndAdd<ShowStatsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowServiceClients: {
      return pool->makeAndAdd<ShowServiceClientsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowFTIndexes: {
      return pool->makeAndAdd<ShowFTIndexesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSignInService: {
      return pool->makeAndAdd<SignInServiceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kSignOutService: {
      return pool->makeAndAdd<SignOutServiceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowSessions: {
      return pool->makeAndAdd<ShowSessionsExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUpdateSession: {
      return pool->makeAndAdd<UpdateSessionExecutor>(node, qctx);
    }
    case PlanNode::Kind::kKillSession: {
      return pool->makeAndAdd<KillSessionExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShowQueries: {
      return pool->makeAndAdd<ShowQueriesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kKillQuery: {
      return pool->makeAndAdd<KillQueryExecutor>(node, qctx);
    }
    case PlanNode::Kind::kTraverse: {
      return pool->makeAndAdd<TraverseExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAppendVertices: {
      return pool->makeAndAdd<AppendVerticesExecutor>(node, qctx);
    }
    case PlanNode::Kind::kHashLeftJoin: {
      return pool->makeAndAdd<HashLeftJoinExecutor>(node, qctx);
    }
    case PlanNode::Kind::kHashInnerJoin: {
      return pool->makeAndAdd<HashInnerJoinExecutor>(node, qctx);
    }
    case PlanNode::Kind::kCrossJoin: {
      return pool->makeAndAdd<CrossJoinExecutor>(node, qctx);
    }
    case PlanNode::Kind::kRollUpApply: {
      return pool->makeAndAdd<RollUpApplyExecutor>(node, qctx);
    }
    case PlanNode::Kind::kPatternApply: {
      return pool->makeAndAdd<PatternApplyExecutor>(node, qctx);
    }
    case PlanNode::Kind::kArgument: {
      return pool->makeAndAdd<ArgumentExecutor>(node, qctx);
    }
    case PlanNode::Kind::kAlterSpace: {
      return pool->makeAndAdd<AlterSpaceExecutor>(node, qctx);
    }
    case PlanNode::Kind::kShortestPath: {
      return pool->makeAndAdd<ShortestPathExecutor>(node, qctx);
    }
    case PlanNode::Kind::kUnknown: {
      DLOG(FATAL) << "Unknown plan node kind " << static_cast<int32_t>(node->kind());
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
  DCHECK(ectx_->exist(node->outputVar()));
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
  // MemoryTrackerVerified

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
  if (node_->isQueryNode() && memory::MemoryUtils::kHitMemoryHighWatermark.load()) {
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

bool Executor::movable(const Variable *var) {
  // Only support input variables of current executor
  DCHECK(std::find(node_->inputVars().begin(), node_->inputVars().end(), DCHECK_NOTNULL(var)) !=
         node_->inputVars().end());
  // TODO support executor in loop
  if (node()->kind() == PlanNode::Kind::kLoop) {
    return false;
  }
  if (node()->loopLayers() != 0) {
    // Guaranteed forward compatibility of go statement execution behavior
    if (node()->kind() == PlanNode::Kind::kFilter) {
      return true;
    }
    // The lifetime of loop body is managed by Loop node
    return false;
  }

  if (node()->kind() == PlanNode::Kind::kSelect) {
    return false;
  }
  // Normal node
  // Make sure drop happened-after count decrement
  return var->userCount.load(std::memory_order_acquire) == 1;
}

Status Executor::finish(Result &&result) {
  // MemoryTrackerVerified
  if (!FLAGS_enable_lifetime_optimize ||
      node()->outputVarPtr()->userCount.load(std::memory_order_relaxed) != 0) {
    numRows_ = result.size();
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

size_t Executor::getBatchSize(size_t totalSize) const {
  // batch size should be the greater one of FLAGS_min_batch_size and (totalSize/FLAGS_max_job_size)
  size_t jobSize = FLAGS_max_job_size;
  size_t minBatchSize = FLAGS_min_batch_size;
  size_t batchSizeTmp = std::ceil(static_cast<float>(totalSize) / jobSize);
  size_t batchSize = batchSizeTmp > minBatchSize ? batchSizeTmp : minBatchSize;
  return batchSize;
}

}  // namespace graph
}  // namespace nebula
