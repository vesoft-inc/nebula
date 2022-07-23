/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/Validator.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/function/FunctionManager.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/validator/ACLValidator.h"
#include "graph/validator/AdminJobValidator.h"
#include "graph/validator/AdminValidator.h"
#include "graph/validator/AssignmentValidator.h"
#include "graph/validator/ExplainValidator.h"
#include "graph/validator/FetchEdgesValidator.h"
#include "graph/validator/FetchVerticesValidator.h"
#include "graph/validator/FindPathValidator.h"
#include "graph/validator/GetSubgraphValidator.h"
#include "graph/validator/GoValidator.h"
#include "graph/validator/GroupByValidator.h"
#include "graph/validator/LimitValidator.h"
#include "graph/validator/LookupValidator.h"
#include "graph/validator/MaintainValidator.h"
#include "graph/validator/MatchValidator.h"
#include "graph/validator/MutateValidator.h"
#include "graph/validator/OrderByValidator.h"
#include "graph/validator/PipeValidator.h"
#include "graph/validator/ReportError.h"
#include "graph/validator/SequentialValidator.h"
#include "graph/validator/SetValidator.h"
#include "graph/validator/UnwindValidator.h"
#include "graph/validator/UseValidator.h"
#include "graph/validator/YieldValidator.h"
#include "graph/visitor/DeduceTypeVisitor.h"
#include "graph/visitor/EvaluableExprVisitor.h"
#include "parser/Sentence.h"

namespace nebula {
namespace graph {

Validator::Validator(Sentence* sentence, QueryContext* qctx)
    : sentence_(DCHECK_NOTNULL(sentence)),
      qctx_(DCHECK_NOTNULL(qctx)),
      vctx_(DCHECK_NOTNULL(qctx->vctx())) {}

// Create validator according to sentence type.
std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, QueryContext* context) {
  auto kind = sentence->kind();
  switch (kind) {
    case Sentence::Kind::kExplain:
      return std::make_unique<ExplainValidator>(sentence, context);
    case Sentence::Kind::kSequential:
      return std::make_unique<SequentialValidator>(sentence, context);
    case Sentence::Kind::kGo:
      return std::make_unique<GoValidator>(sentence, context);
    case Sentence::Kind::kPipe:
      return std::make_unique<PipeValidator>(sentence, context);
    case Sentence::Kind::kAssignment:
      return std::make_unique<AssignmentValidator>(sentence, context);
    case Sentence::Kind::kSet:
      return std::make_unique<SetValidator>(sentence, context);
    case Sentence::Kind::kUse:
      return std::make_unique<UseValidator>(sentence, context);
    case Sentence::Kind::kGetSubgraph:
      return std::make_unique<GetSubgraphValidator>(sentence, context);
    case Sentence::Kind::kLimit:
      return std::make_unique<LimitValidator>(sentence, context);
    case Sentence::Kind::kOrderBy:
      return std::make_unique<OrderByValidator>(sentence, context);
    case Sentence::Kind::kYield:
      return std::make_unique<YieldValidator>(sentence, context);
    case Sentence::Kind::kGroupBy:
      return std::make_unique<GroupByValidator>(sentence, context);
    case Sentence::Kind::kCreateSpace:
      return std::make_unique<CreateSpaceValidator>(sentence, context);
    case Sentence::Kind::kCreateSpaceAs:
      return std::make_unique<CreateSpaceAsValidator>(sentence, context);
    case Sentence::Kind::kCreateTag:
      return std::make_unique<CreateTagValidator>(sentence, context);
    case Sentence::Kind::kCreateEdge:
      return std::make_unique<CreateEdgeValidator>(sentence, context);
    case Sentence::Kind::kDescribeSpace:
      return std::make_unique<DescSpaceValidator>(sentence, context);
    case Sentence::Kind::kDescribeTag:
      return std::make_unique<DescTagValidator>(sentence, context);
    case Sentence::Kind::kDescribeEdge:
      return std::make_unique<DescEdgeValidator>(sentence, context);
    case Sentence::Kind::kAlterTag:
      return std::make_unique<AlterTagValidator>(sentence, context);
    case Sentence::Kind::kAlterEdge:
      return std::make_unique<AlterEdgeValidator>(sentence, context);
    case Sentence::Kind::kShowSpaces:
      return std::make_unique<ShowSpacesValidator>(sentence, context);
    case Sentence::Kind::kShowTags:
      return std::make_unique<ShowTagsValidator>(sentence, context);
    case Sentence::Kind::kShowEdges:
      return std::make_unique<ShowEdgesValidator>(sentence, context);
    case Sentence::Kind::kDropSpace:
      return std::make_unique<DropSpaceValidator>(sentence, context);
    case Sentence::Kind::kDropTag:
      return std::make_unique<DropTagValidator>(sentence, context);
    case Sentence::Kind::kDropEdge:
      return std::make_unique<DropEdgeValidator>(sentence, context);
    case Sentence::Kind::kShowCreateSpace:
      return std::make_unique<ShowCreateSpaceValidator>(sentence, context);
    case Sentence::Kind::kShowCreateTag:
      return std::make_unique<ShowCreateTagValidator>(sentence, context);
    case Sentence::Kind::kShowCreateEdge:
      return std::make_unique<ShowCreateEdgeValidator>(sentence, context);
    case Sentence::Kind::kInsertVertices:
      return std::make_unique<InsertVerticesValidator>(sentence, context);
    case Sentence::Kind::kInsertEdges:
      return std::make_unique<InsertEdgesValidator>(sentence, context);
    case Sentence::Kind::kCreateUser:
      return std::make_unique<CreateUserValidator>(sentence, context);
    case Sentence::Kind::kDropUser:
      return std::make_unique<DropUserValidator>(sentence, context);
    case Sentence::Kind::kAlterUser:
      return std::make_unique<UpdateUserValidator>(sentence, context);
    case Sentence::Kind::kShowUsers:
      return std::make_unique<ShowUsersValidator>(sentence, context);
    case Sentence::Kind::kChangePassword:
      return std::make_unique<ChangePasswordValidator>(sentence, context);
    case Sentence::Kind::kGrant:
      return std::make_unique<GrantRoleValidator>(sentence, context);
    case Sentence::Kind::kRevoke:
      return std::make_unique<RevokeRoleValidator>(sentence, context);
    case Sentence::Kind::kShowRoles:
      return std::make_unique<ShowRolesInSpaceValidator>(sentence, context);
    case Sentence::Kind::kDescribeUser:
      return std::make_unique<DescribeUserValidator>(sentence, context);
    case Sentence::Kind::kAdminJob:
    case Sentence::Kind::kAdminShowJobs:
      return std::make_unique<AdminJobValidator>(sentence, context);
    case Sentence::Kind::kFetchVertices:
      return std::make_unique<FetchVerticesValidator>(sentence, context);
    case Sentence::Kind::kFetchEdges:
      return std::make_unique<FetchEdgesValidator>(sentence, context);
    case Sentence::Kind::kCreateSnapshot:
      return std::make_unique<CreateSnapshotValidator>(sentence, context);
    case Sentence::Kind::kDropSnapshot:
      return std::make_unique<DropSnapshotValidator>(sentence, context);
    case Sentence::Kind::kShowSnapshots:
      return std::make_unique<ShowSnapshotsValidator>(sentence, context);
    case Sentence::Kind::kDeleteVertices:
      return std::make_unique<DeleteVerticesValidator>(sentence, context);
    case Sentence::Kind::kDeleteTags:
      return std::make_unique<DeleteTagsValidator>(sentence, context);
    case Sentence::Kind::kDeleteEdges:
      return std::make_unique<DeleteEdgesValidator>(sentence, context);
    case Sentence::Kind::kUpdateVertex:
      return std::make_unique<UpdateVertexValidator>(sentence, context);
    case Sentence::Kind::kUpdateEdge:
      return std::make_unique<UpdateEdgeValidator>(sentence, context);
    case Sentence::Kind::kAddHosts:
      return std::make_unique<AddHostsValidator>(sentence, context);
    case Sentence::Kind::kDropHosts:
      return std::make_unique<DropHostsValidator>(sentence, context);
    case Sentence::Kind::kShowHosts:
      return std::make_unique<ShowHostsValidator>(sentence, context);
    case Sentence::Kind::kShowMetaLeader:
      return std::make_unique<ShowMetaLeaderValidator>(sentence, context);
    case Sentence::Kind::kShowParts:
      return std::make_unique<ShowPartsValidator>(sentence, context);
    case Sentence::Kind::kShowCharset:
      return std::make_unique<ShowCharsetValidator>(sentence, context);
    case Sentence::Kind::kShowCollation:
      return std::make_unique<ShowCollationValidator>(sentence, context);
    case Sentence::Kind::kGetConfig:
      return std::make_unique<GetConfigValidator>(sentence, context);
    case Sentence::Kind::kSetConfig:
      return std::make_unique<SetConfigValidator>(sentence, context);
    case Sentence::Kind::kShowConfigs:
      return std::make_unique<ShowConfigsValidator>(sentence, context);
    case Sentence::Kind::kFindPath:
      return std::make_unique<FindPathValidator>(sentence, context);
    case Sentence::Kind::kMatch:
      return std::make_unique<MatchValidator>(sentence, context);
    case Sentence::Kind::kCreateTagIndex:
      return std::make_unique<CreateTagIndexValidator>(sentence, context);
    case Sentence::Kind::kShowCreateTagIndex:
      return std::make_unique<ShowCreateTagIndexValidator>(sentence, context);
    case Sentence::Kind::kDescribeTagIndex:
      return std::make_unique<DescribeTagIndexValidator>(sentence, context);
    case Sentence::Kind::kShowTagIndexes:
      return std::make_unique<ShowTagIndexesValidator>(sentence, context);
    case Sentence::Kind::kShowTagIndexStatus:
      return std::make_unique<ShowTagIndexStatusValidator>(sentence, context);
    case Sentence::Kind::kDropTagIndex:
      return std::make_unique<DropTagIndexValidator>(sentence, context);
    case Sentence::Kind::kCreateEdgeIndex:
      return std::make_unique<CreateEdgeIndexValidator>(sentence, context);
    case Sentence::Kind::kShowCreateEdgeIndex:
      return std::make_unique<ShowCreateEdgeIndexValidator>(sentence, context);
    case Sentence::Kind::kDescribeEdgeIndex:
      return std::make_unique<DescribeEdgeIndexValidator>(sentence, context);
    case Sentence::Kind::kShowEdgeIndexes:
      return std::make_unique<ShowEdgeIndexesValidator>(sentence, context);
    case Sentence::Kind::kShowEdgeIndexStatus:
      return std::make_unique<ShowEdgeIndexStatusValidator>(sentence, context);
    case Sentence::Kind::kDropEdgeIndex:
      return std::make_unique<DropEdgeIndexValidator>(sentence, context);
    case Sentence::Kind::kLookup:
      return std::make_unique<LookupValidator>(sentence, context);
    case Sentence::Kind::kMergeZone:
      return std::make_unique<MergeZoneValidator>(sentence, context);
    case Sentence::Kind::kRenameZone:
      return std::make_unique<RenameZoneValidator>(sentence, context);
    case Sentence::Kind::kDropZone:
      return std::make_unique<DropZoneValidator>(sentence, context);
    case Sentence::Kind::kDivideZone:
      return std::make_unique<DivideZoneValidator>(sentence, context);
    case Sentence::Kind::kDescribeZone:
      return std::make_unique<DescribeZoneValidator>(sentence, context);
    case Sentence::Kind::kListZones:
      return std::make_unique<ShowZonesValidator>(sentence, context);
    case Sentence::Kind::kAddHostsIntoZone:
      return std::make_unique<AddHostsIntoZoneValidator>(sentence, context);
    case Sentence::Kind::kAddListener:
      return std::make_unique<AddListenerValidator>(sentence, context);
    case Sentence::Kind::kRemoveListener:
      return std::make_unique<RemoveListenerValidator>(sentence, context);
    case Sentence::Kind::kShowListener:
      return std::make_unique<ShowListenerValidator>(sentence, context);
    case Sentence::Kind::kShowStats:
      return std::make_unique<ShowStatusValidator>(sentence, context);
    case Sentence::Kind::kShowServiceClients:
      return std::make_unique<ShowServiceClientsValidator>(sentence, context);
    case Sentence::Kind::kShowFTIndexes:
      return std::make_unique<ShowFTIndexesValidator>(sentence, context);
    case Sentence::Kind::kSignInService:
      return std::make_unique<SignInServiceValidator>(sentence, context);
    case Sentence::Kind::kSignOutService:
      return std::make_unique<SignOutServiceValidator>(sentence, context);
    case Sentence::Kind::kCreateFTIndex:
      return std::make_unique<CreateFTIndexValidator>(sentence, context);
    case Sentence::Kind::kDropFTIndex:
      return std::make_unique<DropFTIndexValidator>(sentence, context);
    case Sentence::Kind::kShowGroups:
    case Sentence::Kind::kShowZones:
    case Sentence::Kind::kShowSessions:
      return std::make_unique<ShowSessionsValidator>(sentence, context);
    case Sentence::Kind::kShowQueries:
      return std::make_unique<ShowQueriesValidator>(sentence, context);
    case Sentence::Kind::kKillQuery:
      return std::make_unique<KillQueryValidator>(sentence, context);
    case Sentence::Kind::kAlterSpace:
      return std::make_unique<AlterSpaceValidator>(sentence, context);
    case Sentence::Kind::kClearSpace:
      return std::make_unique<ClearSpaceValidator>(sentence, context);
    case Sentence::Kind::kUnwind:
      return std::make_unique<UnwindValidator>(sentence, context);
    case Sentence::Kind::kUnknown:
    case Sentence::Kind::kReturn: {
      // nothing
      DLOG(FATAL) << "Unimplemented sentence " << kind;
    }
  }
  DLOG(FATAL) << "Unknown sentence " << static_cast<int>(kind);
  return std::make_unique<ReportError>(sentence, context);
}

// Entry of validating sentence.
// Check session, switch space of validator context, create validators and validate.
// static
Status Validator::validate(Sentence* sentence, QueryContext* qctx) {
  DCHECK(sentence != nullptr);
  DCHECK(qctx != nullptr);

  // Check if space chosen from session. if chosen, add it to context.
  auto session = qctx->rctx()->session();
  if (session->space().id > kInvalidSpaceID) {
    auto spaceInfo = session->space();
    qctx->vctx()->switchToSpace(std::move(spaceInfo));
  }

  auto validator = makeValidator(sentence, qctx);
  NG_RETURN_IF_ERROR(validator->validate());

  auto root = validator->root();
  if (!root) {
    return Status::SemanticError("Get null plan from sequential validator");
  }
  qctx->plan()->setRoot(root);
  return Status::OK();
}

// Get output columns name of current validator(sentence).
std::vector<std::string> Validator::getOutColNames() const {
  std::vector<std::string> colNames;
  colNames.reserve(outputs_.size());
  for (const auto& col : outputs_) {
    colNames.emplace_back(col.name);
  }
  return colNames;
}

// Append sub-plan to current plan.
// \param node current plan
// \param appended sub-plan to append
Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
  DCHECK(node != nullptr);
  DCHECK(appended != nullptr);

  if (!node->isSingleInput()) {
    return Status::SemanticError("PlanNode(%s) not support to append an input.",
                                 PlanNode::toString(node->kind()));
  }
  static_cast<SingleDependencyNode*>(node)->dependsOn(appended);
  return Status::OK();
}

// Append sub-plan to previous plan
// \param root sub-plan to append
Status Validator::appendPlan(PlanNode* root) {
  return appendPlan(tail_, root);
}

// Validate current sentence.
// Check validator context, space, validate, duplicate reference columns,
// check permission according to sentence kind and privilege of user.
Status Validator::validate() {
  if (!vctx_) {
    VLOG(1) << "Validate context was not given.";
    return Status::SemanticError("Validate context was not given.");
  }

  if (!sentence_) {
    VLOG(1) << "Sentence was not given";
    return Status::SemanticError("Sentence was not given");
  }

  if (!noSpaceRequired_ && !spaceChosen()) {
    VLOG(1) << "Space was not chosen.";
    return Status::SemanticError("Space was not chosen.");
  }

  if (!noSpaceRequired_) {
    space_ = vctx_->whichSpace();
    VLOG(1) << "Space chosen, name: " << space_.spaceDesc.space_name_ref().value()
            << " id: " << space_.id;
  }

  auto vidType = space_.spaceDesc.vid_type_ref().value().type_ref().value();
  vidType_ = SchemaUtil::propTypeToValueType(vidType);

  NG_RETURN_IF_ERROR(validateImpl());

  // Check for duplicate reference column names in pipe or var statement
  NG_RETURN_IF_ERROR(checkDuplicateColName());

  // Execute after validateImpl because need field from it
  if (FLAGS_enable_authorize) {
    NG_RETURN_IF_ERROR(checkPermission());
  }

  NG_RETURN_IF_ERROR(toPlan());

  return Status::OK();
}

// Does one space chosen?
bool Validator::spaceChosen() {
  return vctx_->spaceChosen();
}

// Deduce type of expression.
StatusOr<Value::Type> Validator::deduceExprType(const Expression* expr) const {
  DeduceTypeVisitor visitor(qctx_, vctx_, inputs_, space_.id);
  const_cast<Expression*>(expr)->accept(&visitor);
  if (!visitor.ok()) {
    return std::move(visitor).status();
  }
  return visitor.type();
}

// Collect properties used in expression.
Status Validator::deduceProps(const Expression* expr,
                              ExpressionProps& exprProps,
                              std::vector<TagID>* tagIds,
                              std::vector<EdgeType>* edgeTypes) {
  DeducePropsVisitor visitor(
      qctx_, space_.id, &exprProps, &userDefinedVarNameList_, tagIds, edgeTypes);
  const_cast<Expression*>(expr)->accept(&visitor);
  return std::move(visitor).status();
}

// Call planner to get final execution plan.
Status Validator::toPlan() {
  auto* astCtx = getAstContext();
  if (astCtx != nullptr) {
    astCtx->space = space_;
  }
  auto subPlanStatus = Planner::toPlan(astCtx);
  NG_RETURN_IF_ERROR(subPlanStatus);
  auto subPlan = std::move(subPlanStatus).value();
  root_ = subPlan.root;
  tail_ = subPlan.tail;
  VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
  return Status::OK();
}

// Disable duplicate columns of Input(Variable).
Status Validator::checkDuplicateColName() {
  auto checkColName = [](const ColsDef& nameList) {
    std::unordered_set<std::string> names;
    for (auto& item : nameList) {
      auto ret = names.emplace(item.name);
      if (!ret.second) {
        return Status::SemanticError("Duplicate Column Name : `%s'", item.name.c_str());
      }
    }
    return Status::OK();
  };
  if (!inputs_.empty()) {
    return checkColName(inputs_);
  }
  if (userDefinedVarNameList_.empty()) {
    return Status::OK();
  }
  for (const auto& varName : userDefinedVarNameList_) {
    auto& varProps = vctx_->getVar(varName);
    if (!varProps.empty()) {
      auto res = checkColName(varProps);
      if (!res.ok()) {
        return res;
      }
    }
  }
  return Status::OK();
}

// TODO(Aiee) Move to validateUtil
// Validate and build start vids.
// Check vid type, construct expression to access vid.
Status Validator::validateStarts(const VerticesClause* clause, Starts& starts) {
  if (clause == nullptr) {
    return Status::SemanticError("From clause nullptr.");
  }
  if (clause->isRef()) {
    auto* src = clause->ref();
    if (src->kind() != Expression::Kind::kInputProperty &&
        src->kind() != Expression::Kind::kVarProperty) {
      return Status::SemanticError(
          "`%s', Only input and variable expression is acceptable"
          " when starts are evaluated at runtime.",
          src->toString().c_str());
    }
    starts.fromType = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
    auto res = deduceExprType(src);
    if (!res.ok()) {
      return res.status();
    }
    auto vidType = space_.spaceDesc.vid_type_ref().value().get_type();
    auto& type = res.value();
    if (type != Value::Type::__EMPTY__ && type != SchemaUtil::propTypeToValueType(vidType)) {
      std::stringstream ss;
      ss << "`" << src->toString() << "', the srcs should be type of "
         << apache::thrift::util::enumNameSafe(vidType) << ", but was`" << type << "'";
      return Status::SemanticError(ss.str());
    }
    starts.originalSrc = src;
    auto* propExpr = static_cast<PropertyExpression*>(src);
    if (starts.fromType == kVariable) {
      starts.userDefinedVarName = propExpr->sym();
      userDefinedVarNameList_.emplace(starts.userDefinedVarName);
    }
    starts.runtimeVidName = propExpr->prop();
  } else {
    auto vidList = clause->vidList();
    QueryExpressionContext ctx;
    for (auto* expr : vidList) {
      if (!ExpressionUtils::isEvaluableExpr(expr, qctx_)) {
        return Status::SemanticError("`%s' is not an evaluable expression.",
                                     expr->toString().c_str());
      }
      auto vid = expr->eval(ctx(nullptr));
      auto vidType = space_.spaceDesc.vid_type_ref().value().get_type();
      if (!SchemaUtil::isValidVid(vid, vidType)) {
        std::stringstream ss;
        ss << "Vid should be a " << apache::thrift::util::enumNameSafe(vidType);
        return Status::SemanticError(ss.str());
      }
      starts.vids.emplace_back(std::move(vid));
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
