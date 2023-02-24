/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/GoPlanner.h"

#include "graph/planner/plan/Logic.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/PlannerUtil.h"

namespace nebula {
namespace graph {

std::unique_ptr<GoPlanner::EdgeProps> GoPlanner::buildEdgeProps(bool onlyDst) {
  auto eProps = std::make_unique<EdgeProps>();
  switch (goCtx_->over.direction) {
    case storage::cpp2::EdgeDirection::IN_EDGE: {
      doBuildEdgeProps(eProps, onlyDst, true);
      break;
    }
    case storage::cpp2::EdgeDirection::OUT_EDGE: {
      doBuildEdgeProps(eProps, onlyDst, false);
      break;
    }
    case storage::cpp2::EdgeDirection::BOTH: {
      doBuildEdgeProps(eProps, onlyDst, true);
      doBuildEdgeProps(eProps, onlyDst, false);
      break;
    }
  }
  return eProps;
}

void GoPlanner::doBuildEdgeProps(std::unique_ptr<EdgeProps>& eProps, bool onlyDst, bool isInEdge) {
  const auto& exprProps = goCtx_->exprProps;
  for (const auto& e : goCtx_->over.edgeTypes) {
    EdgeProp ep;
    if (isInEdge) {
      ep.type_ref() = -e;
    } else {
      ep.type_ref() = e;
    }

    if (onlyDst) {
      ep.props_ref() = {kDst};
      eProps->emplace_back(std::move(ep));
      continue;
    }

    auto found = exprProps.edgeProps().find(e);
    if (found == exprProps.edgeProps().end()) {
      ep.props_ref() = {kDst};
    } else {
      std::set<folly::StringPiece> props(found->second.begin(), found->second.end());
      props.emplace(kDst);
      ep.props_ref() = std::vector<std::string>(props.begin(), props.end());
    }
    eProps->emplace_back(std::move(ep));
  }
}

std::unique_ptr<GoPlanner::VertexProps> GoPlanner::buildVertexProps(
    const ExpressionProps::TagIDPropsMap& propsMap) {
  if (propsMap.empty()) {
    return nullptr;
  }
  auto vertexProps = std::make_unique<VertexProps>(propsMap.size());
  auto fun = [](auto& tag) {
    VertexProp vp;
    vp.tag_ref() = tag.first;
    std::vector<std::string> props(tag.second.begin(), tag.second.end());
    vp.props_ref() = std::move(props);
    return vp;
  };
  std::transform(propsMap.begin(), propsMap.end(), vertexProps->begin(), fun);
  return vertexProps;
}

// ++loopSteps{0} <= steps  && (var is Empty OR size(var) != 0)
Expression* GoPlanner::loopCondition(uint32_t steps, const std::string& var) {
  auto* qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();

  qctx->ectx()->setValue(loopStepVar_, 0);
  auto step = ExpressionUtils::stepCondition(pool, loopStepVar_, steps);
  auto empty = ExpressionUtils::equalCondition(pool, var, Value::kEmpty);
  auto neZero = ExpressionUtils::neZeroCondition(pool, var);
  auto* earlyEnd = LogicalExpression::makeOr(pool, empty, neZero);
  return LogicalExpression::makeAnd(pool, step, earlyEnd);
}

// Extracts vid and edge's prop from GN for joinDst & joinInput.
// The root plan node will output colNames of {srcProps, edgeProps, kVid, "JOIN_DST_VID"}.
PlanNode* GoPlanner::extractSrcEdgePropsFromGN(PlanNode* dep, const std::string& input) {
  auto srcEdgePropsExpr = goCtx_->edgePropsExpr->clone();
  // if (goCtx_->srcPropsExpr) {
  //   for (auto col : goCtx_->srcPropsExpr)
  // }
  auto* pool = goCtx_->qctx->objPool();

  if (goCtx_->joinInput) {
    // extract vid from gn
    auto* expr = new YieldColumn(ColumnExpression::make(pool, VID_INDEX), kVid);
    srcEdgePropsExpr->addColumn(expr);
  }

  if (goCtx_->joinDst) {
    // extract dst from gn
    auto* expr = new YieldColumn(EdgePropertyExpression::make(pool, "*", kDst), "JOIN_DST_VID");
    srcEdgePropsExpr->addColumn(expr);
  }

  auto* project = Project::make(goCtx_->qctx, dep, srcEdgePropsExpr.get());
  project->setInputVar(input);
  return project;
}

// Extracts vid and dst from GN for trackStartVid.
// The root plan node will output ColNames of {srcVidColName, "TRACK_DST_VID"}.
PlanNode* GoPlanner::extractSrcDstFromGN(PlanNode* dep, const std::string& input) {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();
  auto* columns = pool->makeAndAdd<YieldColumns>();

  goCtx_->srcVidColName = qctx->vctx()->anonColGen()->getCol();
  auto* vidExpr = new YieldColumn(ColumnExpression::make(pool, VID_INDEX), goCtx_->srcVidColName);
  columns->addColumn(vidExpr);
  auto* dstExpr = new YieldColumn(EdgePropertyExpression::make(pool, "*", kDst), "TRACK_DST_VID");
  columns->addColumn(dstExpr);

  auto* project = Project::make(qctx, dep, columns);
  project->setInputVar(input);
  auto* dedup = Dedup::make(qctx, project);
  return dedup;
}

// Extracts vid from runTime input for joinInput.
// The root plan node will output ColNames of {runtimeVidName, dstVidColName}.
PlanNode* GoPlanner::extractVidFromRuntimeInput(PlanNode* dep) {
  if (dep == nullptr) {
    return dep;
  }
  auto qctx = goCtx_->qctx;
  const auto& from = goCtx_->from;

  auto* columns = qctx->objPool()->makeAndAdd<YieldColumns>();
  auto* vidExpr = new YieldColumn(from.originalSrc->clone(), from.runtimeVidName);
  columns->addColumn(vidExpr);

  goCtx_->dstVidColName = qctx->vctx()->anonColGen()->getCol();
  auto* dstExpr = new YieldColumn(from.originalSrc->clone(), goCtx_->dstVidColName);
  columns->addColumn(dstExpr);

  auto* project = Project::make(qctx, dep, columns);
  auto input = from.fromType == kPipe ? goCtx_->inputVarName : from.userDefinedVarName;
  project->setInputVar(input);

  auto* dedup = Dedup::make(qctx, project);
  return dedup;
}

// Establishes a mapping between the original vId and the expanded destination vId
// during each step of the expansion in the n-step and mton-step scenario.
// The root plan node will output ColNames of {runtimeVidName, dstVidColName}.
// left: (n-1)th step
// right: (n)th step
PlanNode* GoPlanner::trackStartVid(PlanNode* left, PlanNode* right) {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();

  auto* hashKey = VariablePropertyExpression::make(pool, left->outputVar(), goCtx_->dstVidColName);
  auto* probeKey =
      VariablePropertyExpression::make(pool, right->outputVar(), goCtx_->srcVidColName);

  auto* join = HashInnerJoin::make(qctx, left, right, {hashKey}, {probeKey});
  std::vector<std::string> colNames = left->colNames();
  colNames.insert(colNames.end(), right->colNames().begin(), right->colNames().end());
  join->setColNames(std::move(colNames));

  // extract runtimeVid  & dst from join result
  auto* columns = pool->makeAndAdd<YieldColumns>();
  auto& vidName = goCtx_->from.runtimeVidName;
  auto* vidExpr = new YieldColumn(InputPropertyExpression::make(pool, vidName), vidName);
  columns->addColumn(vidExpr);
  auto* dstExpr =
      new YieldColumn(InputPropertyExpression::make(pool, "TRACK_DST_VID"), goCtx_->dstVidColName);
  columns->addColumn(dstExpr);

  auto* project = Project::make(qctx, join, columns);
  auto* dedup = Dedup::make(qctx, project);
  dedup->setOutputVar(left->outputVar());

  return dedup;
}

// The root plan node will output ColNames of
// {srcProps, edgeProps, kVid, "JOIN_DST_VID", "DST_VID", dstProps}
PlanNode* GoPlanner::buildJoinDstPlan(PlanNode* dep) {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();

  auto& colName = dep->colNames().back();
  auto argNode = Argument::make(qctx, colName);
  argNode->setColNames({colName});

  // dst is the first column, columnName is "JOIN_DST_VID"
  auto* dstExpr = ColumnExpression::make(pool, LAST_COL_INDEX);
  auto* getVertex = GetVertices::make(qctx,
                                      argNode,
                                      goCtx_->space.id,
                                      dstExpr,
                                      buildVertexProps(goCtx_->exprProps.dstTagProps()),
                                      {},
                                      true);

  auto& dstPropsExpr = goCtx_->dstPropsExpr;
  // extract dst's prop
  auto* vidExpr = new YieldColumn(ColumnExpression::make(pool, VID_INDEX), "_getVertex_vid");
  dstPropsExpr->addColumn(vidExpr);

  // extract dst's prop, vid is the last column
  auto* project = Project::make(qctx, getVertex, dstPropsExpr);

  // dep's colName "JOIN_DST_VID"  join getVertex's colName "DST_VID"
  auto* hashKey = ColumnExpression::make(pool, LAST_COL_INDEX);
  auto* probeKey = ColumnExpression::make(pool, LAST_COL_INDEX);

  auto* join = HashLeftJoin::make(qctx, dep, project, {hashKey}, {probeKey});
  DLOG(INFO) << join->outputVar() << " hasKey: " << hashKey->toString()
             << " probeKey: " << probeKey->toString();

  std::vector<std::string> colNames = dep->colNames();
  colNames.insert(colNames.end(), project->colNames().begin(), project->colNames().end());
  join->setColNames(std::move(colNames));
  return join;
}

PlanNode* GoPlanner::buildJoinInputPlan(PlanNode* dep) {
  auto qctx = goCtx_->qctx;
  const auto& from = goCtx_->from;
  const auto& steps = goCtx_->steps;
  auto* pool = qctx->objPool();

  const auto& vidName = (!steps.isMToN() && steps.steps() == 1) ? kVid : from.runtimeVidName;
  // auto* hashKey = VariablePropertyExpression::make(pool, dep->outputVar(), vidName);
  // auto* probeKey = from.originalSrc;
  auto* hashKey = from.originalSrc;
  auto* probeKey = VariablePropertyExpression::make(pool, dep->outputVar(), vidName);
  auto* join = HashInnerJoin::make(qctx, preRootNode_, dep, {hashKey}, {probeKey});

  auto& varName = from.fromType == kPipe ? goCtx_->inputVarName : from.userDefinedVarName;
  auto* varPtr = qctx->symTable()->getVar(varName);
  DCHECK(varPtr != nullptr);
  std::vector<std::string> colNames = varPtr->colNames;
  colNames.insert(colNames.end(), dep->colNames().begin(), dep->colNames().end());
  join->setColNames(std::move(colNames));

  return join;
}

// The column named as dstVidColName of the left plan node join on the column named as kVid of the
// right one.
// left  : (n-1)th step
// right : last step
PlanNode* GoPlanner::lastStepJoinInput(PlanNode* left, PlanNode* right) {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();

  auto* hashKey = VariablePropertyExpression::make(pool, left->outputVar(), goCtx_->dstVidColName);
  auto* probeKey = VariablePropertyExpression::make(pool, right->outputVar(), kVid);

  const auto& leftVersion = goCtx_->steps.isMToN() ? ExecutionContext::kPreviousOneVersion
                                                   : ExecutionContext::kLatestVersion;
  UNUSED(leftVersion);
  auto* join = HashInnerJoin::make(qctx, left, right, {hashKey}, {probeKey});
  std::vector<std::string> colNames = left->colNames();
  colNames.insert(colNames.end(), right->colNames().begin(), right->colNames().end());
  join->setColNames(std::move(colNames));

  return join;
}

PlanNode* GoPlanner::buildLastStepJoinPlan(PlanNode* gn, PlanNode* join) {
  if (!goCtx_->joinInput && !goCtx_->joinDst) {
    return gn;
  }

  auto* dep = extractSrcEdgePropsFromGN(gn, gn->outputVar());
  dep = goCtx_->joinDst ? buildJoinDstPlan(dep) : dep;
  dep = goCtx_->joinInput ? lastStepJoinInput(join, dep) : dep;
  dep = goCtx_->joinInput ? buildJoinInputPlan(dep) : dep;

  return dep;
}

PlanNode* GoPlanner::lastStep(PlanNode* dep, PlanNode* join) {
  auto qctx = goCtx_->qctx;
  PlanNode* cur = nullptr;

  if (goCtx_->isSimple) {
    auto* gd = GetDstBySrc::make(qctx, dep, goCtx_->space.id);
    gd->setSrc(goCtx_->from.src);
    gd->setEdgeTypes(buildEdgeTypes());
    gd->setInputVar(goCtx_->vidsVar);
    gd->setColNames({goCtx_->dstIdColName});
    auto* dedup = Dedup::make(qctx, gd);
    dedup->setColNames(gd->colNames());
    cur = dedup;

    if (goCtx_->joinDst) {
      cur = buildJoinDstPlan(cur);
    }
    if (goCtx_->filter) {
      cur = Filter::make(qctx, cur, goCtx_->filter);
    }
    if (goCtx_->joinDst || goCtx_->yieldExpr->columns().size() != 1) {
      cur = Project::make(qctx, cur, goCtx_->yieldExpr);
    } else {
      gd->setColNames(goCtx_->colNames);
      dedup->setColNames(goCtx_->colNames);
    }
    cur->setColNames(goCtx_->colNames);
    return cur;
  } else {
    auto* gn = GetNeighbors::make(qctx, dep, goCtx_->space.id);
    gn->setSrc(goCtx_->from.src);
    gn->setVertexProps(buildVertexProps(goCtx_->exprProps.srcTagProps()));
    gn->setEdgeProps(buildEdgeProps(false));
    gn->setInputVar(goCtx_->vidsVar);
    gn->setEdgeDirection(goCtx_->over.direction);

    const auto& steps = goCtx_->steps;
    auto* sampleLimit = buildSampleLimit(gn, steps.isMToN() ? steps.nSteps() : steps.steps());

    auto* root = buildLastStepJoinPlan(sampleLimit, join);

    if (goCtx_->filter != nullptr) {
      root = Filter::make(qctx, root, goCtx_->filter);
    }

    root = Project::make(qctx, root, goCtx_->yieldExpr);
    root->setColNames(std::move(goCtx_->colNames));

    if (goCtx_->distinct) {
      root = Dedup::make(qctx, root);
    }
    return root;
  }
}

PlanNode* GoPlanner::buildOneStepJoinPlan(PlanNode* gn) {
  if (!goCtx_->joinInput && !goCtx_->joinDst) {
    return gn;
  }

  auto* dep = extractSrcEdgePropsFromGN(gn, gn->outputVar());
  dep = goCtx_->joinDst ? buildJoinDstPlan(dep) : dep;
  dep = goCtx_->joinInput ? buildJoinInputPlan(dep) : dep;

  return dep;
}

template <typename T>
PlanNode* GoPlanner::buildSampleLimitImpl(PlanNode* input, T sampleLimit) {
  DCHECK(!goCtx_->limits.empty());
  PlanNode* node = nullptr;
  if (goCtx_->random) {
    node = Sample::make(goCtx_->qctx, input, sampleLimit);
  } else {
    node = Limit::make(goCtx_->qctx, input, 0, sampleLimit);
  }
  node->setInputVar(input->outputVar());
  node->setColNames(input->outputVarPtr()->colNames);
  return node;
}

// Generates $limits[$step-1]
Expression* GoPlanner::stepSampleLimit() {
  auto qctx = goCtx_->qctx;
  // $limits
  const auto& limitsVarName = qctx->vctx()->anonVarGen()->getVar();
  List limitValues;
  limitValues.reserve(goCtx_->limits.size());
  for (const auto& limit : goCtx_->limits) {
    limitValues.values.emplace_back(limit);
  }
  qctx->ectx()->setValue(limitsVarName, Value(std::move(limitValues)));
  auto* limitsVar = VariableExpression::make(qctx->objPool(), limitsVarName);
  // $step
  auto* stepVar = VariableExpression::make(qctx->objPool(), loopStepVar_);
  // step inc
  auto* stepInc = ArithmeticExpression::makeMinus(
      qctx->objPool(), stepVar, ConstantExpression::make(qctx->objPool(), 1));
  // subscript
  auto* subscript = SubscriptExpression::make(qctx->objPool(), limitsVar, stepInc);
  return subscript;
}

SubPlan GoPlanner::oneStepPlan() {
  auto qctx = goCtx_->qctx;
  auto isSimple = goCtx_->isSimple;

  PlanNode* cur = nullptr;
  if (isSimple) {
    auto* gd = GetDstBySrc::make(qctx, startNode_, goCtx_->space.id);
    gd->setSrc(goCtx_->from.src);
    gd->setEdgeTypes(buildEdgeTypes());
    gd->setInputVar(goCtx_->vidsVar);
    gd->setColNames({goCtx_->dstIdColName});

    auto* dedup = Dedup::make(qctx, gd);
    dedup->setColNames(gd->colNames());
    cur = dedup;
    if (goCtx_->joinDst) {
      cur = buildJoinDstPlan(cur);
    }

    if (goCtx_->filter != nullptr) {
      cur = Filter::make(qctx, cur, goCtx_->filter);
    }

    if (goCtx_->joinDst || goCtx_->yieldExpr->columns().size() != 1) {
      cur = Project::make(qctx, cur, goCtx_->yieldExpr);
    } else {
      gd->setColNames(goCtx_->colNames);
      dedup->setColNames(goCtx_->colNames);
    }
    cur->setColNames(std::move(goCtx_->colNames));
  } else {
    auto* gn = GetNeighbors::make(qctx, startNode_, goCtx_->space.id);
    gn->setVertexProps(buildVertexProps(goCtx_->exprProps.srcTagProps()));
    gn->setEdgeProps(buildEdgeProps(false));
    gn->setSrc(goCtx_->from.src);
    gn->setInputVar(goCtx_->vidsVar);

    auto* sampleLimit = buildSampleLimit(gn, 1 /* one step */);
    cur = sampleLimit;
    cur = buildOneStepJoinPlan(sampleLimit);

    if (goCtx_->filter != nullptr) {
      cur = Filter::make(qctx, cur, goCtx_->filter);
    }

    cur = Project::make(qctx, cur, goCtx_->yieldExpr);
    cur->setColNames(std::move(goCtx_->colNames));
    if (goCtx_->distinct) {
      cur = Dedup::make(qctx, cur);
    }
  }

  SubPlan subPlan;
  subPlan.root = cur;
  subPlan.tail = startNode_;
  return subPlan;
}

SubPlan GoPlanner::nStepsPlan() {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();
  loopStepVar_ = qctx->vctx()->anonVarGen()->getVar();
  auto& from = goCtx_->from;

  SubPlan subPlan;
  if (!goCtx_->joinInput && goCtx_->limits.empty()) {
    auto* gd = GetDstBySrc::make(qctx, startNode_, goCtx_->space.id);
    gd->setSrc(from.src);
    gd->setEdgeTypes(buildEdgeTypes());
    gd->setInputVar(goCtx_->vidsVar);
    gd->setColNames({goCtx_->dstIdColName});
    auto* dedup = Dedup::make(qctx, gd);
    dedup->setOutputVar(goCtx_->vidsVar);
    dedup->setColNames(gd->colNames());

    auto* condition = loopCondition(goCtx_->steps.steps() - 1, dedup->outputVar());
    auto* loop = Loop::make(qctx, nullptr, dedup, condition);

    auto* root = lastStep(loop, nullptr);
    subPlan.root = root;
    subPlan.tail = loop;
    return subPlan;
  }
  // n-1 step
  size_t steps = goCtx_->steps.steps();
  auto* expand = Expand::make(
      qctx, startNode_, goCtx_->space.id, goCtx_->random, steps - 1, buildEdgeProps(true));
  expand->setExpression(from.src);
  expand->setColNames({"_expand_vid", "_expand_dst"});
  expand->setLimits(goCtx_->limits);

  // last step
  // joindst add dst column(todo) for joinDst
  // join input add vid column for joinInput

  auto* arg = Argument::make(qctx, "_expand_dst");
  arg->setColNames({"_expand_dst"});

  auto* vidExpr = new YieldColumn(ColumnExpression::make(pool, VID_INDEX), "_expandall_vid");
  goCtx_->srcPropsExpr->addColumn(vidExpr);
  if (goCtx_->joinDst) {
    auto* dstExpr =
        new YieldColumn(EdgePropertyExpression::make(pool, "*", kDst), "_expandall_dst");
    goCtx_->edgePropsExpr->addColumn(dstExpr);
  }
  auto* expandAll = ExpandAll::make(qctx,
                                    arg,
                                    goCtx_->space.id,
                                    goCtx_->random,
                                    steps - 1,
                                    steps,
                                    buildEdgeProps(false),
                                    buildVertexProps(goCtx_->exprProps.srcTagProps()),
                                    goCtx_->srcPropsExpr,
                                    goCtx_->edgePropsExpr);
  expandAll->setLimits(goCtx_->limits);
  PlanNode* dep = expandAll;
  if (goCtx_->joinDst) {
    dep = buildJoinDstPlan(expandAll);
  }

  {
    // n-1 step's dst(_expand_dst) InnerJoin last step's vid(_expandAll_vid)
    // joinLeft : expand   joinRight : expandAll(no need join dst) OR hashLeftJoin
    auto* hashKey = VariablePropertyExpression::make(pool, expand->outputVar(), "_expand_dst");
    auto* probeKey = VariablePropertyExpression::make(pool, dep->outputVar(), "_expandall_vid");
    auto* join = HashInnerJoin::make(qctx, expand, dep, {hashKey}, {probeKey});
    std::vector<std::string> colNames = expand->colNames();
    colNames.insert(colNames.end(), dep->colNames().begin(), dep->colNames().end());
    join->setColNames(std::move(colNames));
    dep = join;
  }

  if (goCtx_->joinInput) {
    // the result of the previous statement InnerJoin the result of the current statement
    auto* hashKey = from.originalSrc;
    auto* probeKey = VariablePropertyExpression::make(pool, dep->outputVar(), "_expand_vid");
    auto* join = HashInnerJoin::make(qctx, preRootNode_, dep, {hashKey}, {probeKey});

    auto& varName = from.fromType == kPipe ? goCtx_->inputVarName : from.userDefinedVarName;
    auto* varPtr = qctx->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    std::vector<std::string> colNames = varPtr->colNames;
    colNames.insert(colNames.end(), dep->colNames().begin(), dep->colNames().end());
    join->setColNames(std::move(colNames));
    dep = join;
  }

  if (goCtx_->filter != nullptr) {
    dep = Filter::make(qctx, dep, goCtx_->filter);
  }

  dep = Project::make(qctx, dep, goCtx_->yieldExpr);
  dep->setColNames(std::move(goCtx_->colNames));

  if (goCtx_->distinct) {
    dep = Dedup::make(qctx, dep);
  }
  subPlan.root = dep;
  subPlan.tail = startNode_;
  return subPlan;
}

SubPlan GoPlanner::mToNStepsPlan() {
  auto qctx = goCtx_->qctx;
  auto joinDst = goCtx_->joinDst;
  auto isSimple = goCtx_->isSimple;

  loopStepVar_ = qctx->vctx()->anonVarGen()->getVar();
  PlanNode* getDst = nullptr;
  PlanNode* loopBody = nullptr;
  SubPlan subPlan;
  if (isSimple) {
    auto* start = StartNode::make(qctx);
    auto* gd = GetDstBySrc::make(qctx, start, goCtx_->space.id);
    gd->setSrc(goCtx_->from.src);
    gd->setEdgeTypes(buildEdgeTypes());
    gd->setInputVar(goCtx_->vidsVar);
    gd->setColNames({goCtx_->dstIdColName});
    auto* dedup = Dedup::make(qctx, gd);
    // The outputVar of `Dedup` is the same as the inputVar of `GetDstBySrc`.
    // So the output of `Dedup` of current iteration feeds into the input of `GetDstBySrc` of next
    // iteration.
    dedup->setOutputVar(goCtx_->vidsVar);
    dedup->setColNames(gd->colNames());
    getDst = dedup;
    loopBody = getDst;

    if (joinDst) {
      loopBody = extractDstId(loopBody);
      // Left join, join the dst id with `GetVertices`
      loopBody = buildJoinDstPlan(loopBody);
    }
    if (goCtx_->filter) {
      loopBody = Filter::make(qctx, loopBody, goCtx_->filter);
    }
    if (joinDst || goCtx_->yieldExpr->columns().size() != 1) {
      loopBody = Project::make(qctx, loopBody, goCtx_->yieldExpr);
    } else {
      gd->setColNames(goCtx_->colNames);
      dedup->setColNames(goCtx_->colNames);
    }
    loopBody->setColNames(goCtx_->colNames);
    auto* condition = loopCondition(goCtx_->steps.nSteps(), getDst->outputVar());
    auto* loop = Loop::make(qctx, nullptr, loopBody, condition);

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kMToN);
    dc->addDep(loop);
    if (loopBody == getDst) {
      StepClause newStep(goCtx_->steps.mSteps() + 1, goCtx_->steps.nSteps() + 1);
      dc->setMToN(newStep);
    } else {
      dc->setMToN(goCtx_->steps);
    }
    dc->setInputVars({loopBody->outputVar()});

    dc->setDistinct(goCtx_->distinct);
    dc->setColNames(goCtx_->colNames);

    subPlan.root = dc;
    subPlan.tail = loop;
    return subPlan;
  }
  return subPlan;
}

StatusOr<SubPlan> GoPlanner::transform(AstContext* astCtx) {
  goCtx_ = static_cast<GoContext*>(astCtx);
  auto qctx = goCtx_->qctx;
  goCtx_->joinInput = goCtx_->from.fromType != FromType::kInstantExpr && !goCtx_->isSimple;
  goCtx_->joinDst = !goCtx_->exprProps.dstTagProps().empty();

  startNode_ = StartNode::make(qctx);
  auto& from = goCtx_->from;
  if (!from.vids.empty() && from.originalSrc == nullptr) {
    PlannerUtil::buildConstantInput(qctx, from, goCtx_->vidsVar);
  } else {
    // get root node of the previous statement
    auto& varName = from.fromType == kVariable ? from.userDefinedVarName : goCtx_->inputVarName;
    auto* varPtr = qctx->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    DCHECK_EQ(varPtr->writtenBy.size(), 1);
    for (auto node : varPtr->writtenBy) {
      preRootNode_ = node;
    }

    auto& colName = from.fromType == kVariable ? from.userDefinedVarName : from.runtimeVidName;
    auto argNode = Argument::make(qctx, colName);
    argNode->setColNames({colName});
    goCtx_->vidsVar = argNode->outputVar();
    startNode_ = argNode;
  }

  auto& steps = goCtx_->steps;
  if (steps.isMToN()) {
    return mToNStepsPlan();
  }

  if (steps.steps() == 0) {
    auto* pt = PassThroughNode::make(qctx, nullptr);
    pt->setColNames(std::move(goCtx_->colNames));
    SubPlan subPlan;
    subPlan.root = subPlan.tail = pt;
    return subPlan;
  }

  if (steps.steps() == 1) {
    return oneStepPlan();
  }
  return nStepsPlan();
}

std::vector<EdgeType> GoPlanner::buildEdgeTypes() {
  switch (goCtx_->over.direction) {
    case storage::cpp2::EdgeDirection::IN_EDGE: {
      std::vector<EdgeType> edgeTypes;
      edgeTypes.reserve(goCtx_->over.edgeTypes.size());
      for (auto edgeType : goCtx_->over.edgeTypes) {
        edgeTypes.emplace_back(-edgeType);
      }
      return edgeTypes;
    }
    case storage::cpp2::EdgeDirection::OUT_EDGE: {
      return goCtx_->over.edgeTypes;
    }
    case storage::cpp2::EdgeDirection::BOTH: {
      std::vector<EdgeType> edgeTypes;
      for (auto edgeType : goCtx_->over.edgeTypes) {
        edgeTypes.emplace_back(edgeType);
        edgeTypes.emplace_back(-edgeType);
      }
      return edgeTypes;
    }
  }
  return {};
}

PlanNode* GoPlanner::extractDstId(PlanNode* node) {
  auto pool = goCtx_->qctx->objPool();
  auto* columns = pool->makeAndAdd<YieldColumns>();
  auto* column = new YieldColumn(ColumnExpression::make(pool, 0));
  columns->addColumn(column);
  auto* project = Project::make(goCtx_->qctx, node, columns);
  project->setColNames({goCtx_->dstIdColName});
  return project;
}

}  // namespace graph
}  // namespace nebula
