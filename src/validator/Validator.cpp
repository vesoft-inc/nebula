/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"

#include "common/function/FunctionManager.h"
#include "parser/Sentence.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "validator/ACLValidator.h"
#include "validator/AdminJobValidator.h"
#include "validator/AdminValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/BalanceValidator.h"
#include "validator/ExplainValidator.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/FindPathValidator.h"
#include "validator/GetSubgraphValidator.h"
#include "validator/GoValidator.h"
#include "validator/GroupByValidator.h"
#include "validator/LimitValidator.h"
#include "validator/MaintainValidator.h"
#include "validator/MutateValidator.h"
#include "validator/OrderByValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/YieldValidator.h"
#include "visitor/DeducePropsVisitor.h"
#include "visitor/DeduceTypeVisitor.h"
#include "validator/GroupByValidator.h"
#include "validator/MatchValidator.h"
#include "visitor/EvaluableExprVisitor.h"
#include "validator/LookupValidator.h"
#include "validator/DownloadValidator.h"
#include "validator/IngestValidator.h"

namespace nebula {
namespace graph {

Validator::Validator(Sentence* sentence, QueryContext* qctx)
    : sentence_(DCHECK_NOTNULL(sentence)),
      qctx_(DCHECK_NOTNULL(qctx)),
      vctx_(DCHECK_NOTNULL(qctx->vctx())) {}

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
        case Sentence::Kind::kBalance:
            return std::make_unique<BalanceValidator>(sentence, context);
        case Sentence::Kind::kAdminJob:
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
        case Sentence::Kind::kDeleteEdges:
            return std::make_unique<DeleteEdgesValidator>(sentence, context);
        case Sentence::Kind::kUpdateVertex:
            return std::make_unique<UpdateVertexValidator>(sentence, context);
        case Sentence::Kind::kUpdateEdge:
            return std::make_unique<UpdateEdgeValidator>(sentence, context);
        case Sentence::Kind::kShowHosts:
            return std::make_unique<ShowHostsValidator>(sentence, context);
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
        case Sentence::Kind::kAddGroup:
            return std::make_unique<AddGroupValidator>(sentence, context);
        case Sentence::Kind::kDropGroup:
            return std::make_unique<DropGroupValidator>(sentence, context);
        case Sentence::Kind::kDescribeGroup:
            return std::make_unique<DescribeGroupValidator>(sentence, context);
        case Sentence::Kind::kListGroups:
            return std::make_unique<ListGroupsValidator>(sentence, context);
        case Sentence::Kind::kAddZoneIntoGroup:
            return std::make_unique<AddZoneIntoGroupValidator>(sentence, context);
        case Sentence::Kind::kDropZoneFromGroup:
            return std::make_unique<DropZoneFromGroupValidator>(sentence, context);
        case Sentence::Kind::kAddZone:
            return std::make_unique<AddZoneValidator>(sentence, context);
        case Sentence::Kind::kDropZone:
            return std::make_unique<DropZoneValidator>(sentence, context);
        case Sentence::Kind::kDescribeZone:
            return std::make_unique<DescribeZoneValidator>(sentence, context);
        case Sentence::Kind::kListZones:
            return std::make_unique<ListZonesValidator>(sentence, context);
        case Sentence::Kind::kAddHostIntoZone:
            return std::make_unique<AddHostIntoZoneValidator>(sentence, context);
        case Sentence::Kind::kDropHostFromZone:
            return std::make_unique<DropHostFromZoneValidator>(sentence, context);
        case Sentence::Kind::kAddListener:
            return std::make_unique<AddListenerValidator>(sentence, context);
        case Sentence::Kind::kRemoveListener:
            return std::make_unique<RemoveListenerValidator>(sentence, context);
        case Sentence::Kind::kShowListener:
            return std::make_unique<ShowListenerValidator>(sentence, context);
        case Sentence::Kind::kShowStats:
            return std::make_unique<ShowStatusValidator>(sentence, context);
        case Sentence::Kind::kShowTSClients:
            return std::make_unique<ShowTSClientsValidator>(sentence, context);
        case Sentence::Kind::kShowFTIndexes:
            return std::make_unique<ShowFTIndexesValidator>(sentence, context);
        case Sentence::Kind::kSignInTSService:
            return std::make_unique<SignInTSServiceValidator>(sentence, context);
        case Sentence::Kind::kSignOutTSService:
            return std::make_unique<SignOutTSServiceValidator>(sentence, context);
        case Sentence::Kind::kDownload:
            return std::make_unique<DownloadValidator>(sentence, context);
        case Sentence::Kind::kIngest:
            return std::make_unique<IngestValidator>(sentence, context);
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
        case Sentence::Kind::kUnknown:
        case Sentence::Kind::kReturn: {
            // nothing
            DLOG(FATAL) << "Unimplemented sentence " << kind;
        }
    }
    DLOG(FATAL) << "Unknown sentence " << static_cast<int>(kind);
    return std::make_unique<ReportError>(sentence, context);
}

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

std::vector<std::string> Validator::getOutColNames() const {
    std::vector<std::string> colNames;
    colNames.reserve(outputs_.size());
    for (const auto& col : outputs_) {
        colNames.emplace_back(col.name);
    }
    return colNames;
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    DCHECK(node != nullptr);
    DCHECK(appended != nullptr);
    if (!node->isSingleInput()) {
        return Status::SemanticError("%s not support to append an input.",
                                     PlanNode::toString(node->kind()));
    }
    static_cast<SingleDependencyNode*>(node)->dependsOn(appended);
    return Status::OK();
}

Status Validator::appendPlan(PlanNode* root) {
    return appendPlan(tail_, root);
}

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

bool Validator::spaceChosen() {
    return vctx_->spaceChosen();
}

StatusOr<Value::Type> Validator::deduceExprType(const Expression* expr) const {
    DeduceTypeVisitor visitor(qctx_, vctx_, inputs_, space_.id);
    const_cast<Expression*>(expr)->accept(&visitor);
    if (!visitor.ok()) {
        return std::move(visitor).status();
    }
    return visitor.type();
}

Status Validator::deduceProps(const Expression* expr, ExpressionProps& exprProps) {
    DeducePropsVisitor visitor(qctx_, space_.id, &exprProps, &userDefinedVarNameList_);
    const_cast<Expression*>(expr)->accept(&visitor);
    return std::move(visitor).status();
}

bool Validator::evaluableExpr(const Expression* expr) const {
    EvaluableExprVisitor visitor;
    const_cast<Expression*>(expr)->accept(&visitor);
    return visitor.ok();
}

StatusOr<std::string> Validator::checkRef(const Expression* ref, Value::Type type) {
    if (ref->kind() == Expression::Kind::kInputProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(propExpr->prop(), type);
        const auto find = std::find(inputs_.begin(), inputs_.end(), col);
        if (find == inputs_.end()) {
            return Status::SemanticError("No input property `%s'", propExpr->prop().c_str());
        }
        return inputVarName_;
    }
    if (ref->kind() == Expression::Kind::kVarProperty) {
        const auto* propExpr = static_cast<const PropertyExpression*>(ref);
        ColDef col(propExpr->prop(), type);

        const auto &outputVar = propExpr->sym();
        const auto &var = vctx_->getVar(outputVar);
        if (var.empty()) {
            return Status::SemanticError("No variable `%s'", outputVar.c_str());
        }
        const auto find = std::find(var.begin(), var.end(), col);
        if (find == var.end()) {
            return Status::SemanticError(
                "No property `%s' in variable `%s'", propExpr->prop().c_str(), outputVar.c_str());
        }
        userDefinedVarNameList_.emplace(outputVar);
        return outputVar;
    }
    // it's guranteed by parser
    DLOG(FATAL) << "Unexpected expression " << ref->kind();
    return Status::SemanticError("Unexpected expression.");
}

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

Status Validator::checkDuplicateColName() {
    auto checkColName = [] (const ColsDef& nameList) {
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

Status Validator::invalidLabelIdentifiers(const Expression* expr) const {
    auto labelExprs = ExpressionUtils::collectAll(expr, {Expression::Kind::kLabel});
    if (!labelExprs.empty()) {
        std::stringstream ss;
        ss << "Invalid label identifiers: ";
        for (auto* label : labelExprs) {
            ss << label->toString() << ",";
        }
        auto errMsg = ss.str();
        errMsg.pop_back();
        return Status::SemanticError(std::move(errMsg));
    }

    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
