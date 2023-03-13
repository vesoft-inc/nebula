/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/PropertyTrackerVisitor.h"

#include "common/expression/Expression.h"
#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

std::string PropertyTracker::toString() const {
  std::string str;
  str += "vertexPropsMap: ";
  for (const auto &v : vertexPropsMap) {
    str += v.first + ": ";
    for (const auto &t : v.second) {
      str += std::to_string(t.first) + ": ";
      for (const auto &p : t.second) {
        str += p + ", ";
      }
    }
  }
  str += "edgePropsMap: ";
  for (const auto &v : edgePropsMap) {
    str += v.first + ": ";
    for (const auto &t : v.second) {
      str += std::to_string(t.first) + ": ";
      for (const auto &p : t.second) {
        str += p + ", ";
      }
    }
  }
  str += "colsSet: ";
  for (const auto &c : colsSet) {
    str += c + ", ";
  }
  return str;
}

void PropertyTracker::insertVertexProp(const std::string &name,
                                       TagID tagId,
                                       const std::string &propName) {
  if (colsSet.find(name) != colsSet.end()) {
    return;
  }
  auto iter = vertexPropsMap.find(name);
  if (iter == vertexPropsMap.end()) {
    vertexPropsMap[name][tagId].emplace(propName);
  } else {
    auto propIter = iter->second.find(tagId);
    if (propIter == iter->second.end()) {
      std::unordered_set<std::string> temp({propName});
      iter->second.emplace(tagId, std::move(temp));
    } else {
      propIter->second.emplace(propName);
    }
  }
}

void PropertyTracker::insertEdgeProp(const std::string &name,
                                     EdgeType type,
                                     const std::string &propName) {
  if (colsSet.find(name) != colsSet.end()) {
    return;
  }
  auto iter = edgePropsMap.find(name);
  if (iter == edgePropsMap.end()) {
    edgePropsMap[name][type].emplace(propName);
  } else {
    auto propIter = iter->second.find(type);
    if (propIter == iter->second.end()) {
      std::unordered_set<std::string> temp({propName});
      iter->second.emplace(type, std::move(temp));
    } else {
      propIter->second.emplace(propName);
    }
  }
}

void PropertyTracker::insertCols(const std::string &name) {
  colsSet.emplace(name);
  vertexPropsMap.erase(name);
  edgePropsMap.erase(name);
}

Status PropertyTracker::update(const std::string &oldName, const std::string &newName) {
  if (oldName == newName) {
    return Status::OK();
  }

  auto it1 = vertexPropsMap.find(oldName);
  bool hasNodeAlias = it1 != vertexPropsMap.end();
  auto it2 = edgePropsMap.find(oldName);
  bool hasEdgeAlias = it2 != edgePropsMap.end();
  if (hasNodeAlias && hasEdgeAlias) {
    return Status::Error("Duplicated property name: %s", oldName.c_str());
  }
  if (hasNodeAlias) {
    if (vertexPropsMap.find(newName) != vertexPropsMap.end()) {
      return Status::Error("Property name %s conflicted with %s", newName.c_str(), oldName.c_str());
    }
    vertexPropsMap[newName] = std::move(it1->second);
    vertexPropsMap.erase(it1);
    colsSet.erase(oldName);
  }
  if (hasEdgeAlias) {
    if (edgePropsMap.find(newName) != edgePropsMap.end()) {
      return Status::Error("Property name %s conflicted with %s", newName.c_str(), oldName.c_str());
    }
    edgePropsMap[newName] = std::move(it2->second);
    edgePropsMap.erase(it2);
    colsSet.erase(oldName);
  }

  auto it3 = colsSet.find(oldName);
  if (it3 != colsSet.end()) {
    colsSet.erase(it3);
    insertCols(newName);
  }

  return Status::OK();
}

bool PropertyTracker::hasAlias(const std::string &name) const {
  return vertexPropsMap.find(name) != vertexPropsMap.end() ||
         edgePropsMap.find(name) != edgePropsMap.end() || colsSet.find(name) != colsSet.end();
}

PropertyTrackerVisitor::PropertyTrackerVisitor(const QueryContext *qctx,
                                               GraphSpaceID space,
                                               PropertyTracker &propsUsed,
                                               const std::string &entityAlias)
    : qctx_(qctx), space_(space), propsUsed_(propsUsed), entityAlias_(entityAlias) {
  DCHECK(qctx != nullptr);
}

void PropertyTrackerVisitor::visit(TagPropertyExpression *expr) {
  auto &tagName = expr->sym();
  auto &propName = expr->prop();
  auto ret = qctx_->schemaMng()->toTagID(space_, tagName);
  if (!ret.ok()) {
    status_ = std::move(ret).status();
    return;
  }
  auto tagId = ret.value();
  propsUsed_.insertVertexProp(entityAlias_, tagId, propName);
}

void PropertyTrackerVisitor::visit(EdgePropertyExpression *expr) {
  auto &edgeName = expr->sym();
  auto &propName = expr->prop();
  auto ret = qctx_->schemaMng()->toEdgeType(space_, edgeName);
  if (!ret.ok()) {
    status_ = std::move(ret).status();
    return;
  }
  auto edgeType = ret.value();
  propsUsed_.insertEdgeProp(entityAlias_, edgeType, propName);
}

void PropertyTrackerVisitor::visit(LabelTagPropertyExpression *expr) {
  auto &nodeAlias = static_cast<VariablePropertyExpression *>(expr->label())->prop();
  auto &tagName = expr->sym();
  auto &propName = expr->prop();

  auto ret = qctx_->schemaMng()->toTagID(space_, tagName);
  if (!ret.ok()) {
    // if the we switch space in the query, we need to get the space id from the validation context
    // use xxx; match xxx
    if (qctx_->vctx()->spaceChosen()) {
      space_ = qctx_->vctx()->whichSpace().id;
      ret = qctx_->schemaMng()->toTagID(qctx_->vctx()->whichSpace().id, tagName);
      if (!ret.ok()) {
        status_ = std::move(ret).status();
        return;
      }
    }
  }

  auto tagId = ret.value();
  propsUsed_.insertVertexProp(nodeAlias, tagId, propName);
}

void PropertyTrackerVisitor::visit(InputPropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.insertCols(colName);
}

void PropertyTrackerVisitor::visit(VariablePropertyExpression *expr) {
  auto &colName = expr->prop();
  propsUsed_.insertCols(colName);
}

void PropertyTrackerVisitor::visit(AttributeExpression *expr) {
  auto *lhs = expr->left();
  auto *rhs = expr->right();
  if (rhs->kind() != Expression::Kind::kConstant) {
    return;
  }
  auto *constExpr = static_cast<ConstantExpression *>(rhs);
  auto &constVal = constExpr->value();
  if (constVal.type() != Value::Type::STRING) {
    return;
  }
  auto &propName = constVal.getStr();
  switch (lhs->kind()) {
    case Expression::Kind::kInputProperty:
    case Expression::Kind::kVarProperty: {  // $e.name
      auto *varPropExpr = static_cast<PropertyExpression *>(lhs);
      auto &edgeAlias = varPropExpr->prop();
      propsUsed_.insertEdgeProp(edgeAlias, unknownType_, propName);
      break;
    }
    case Expression::Kind::kCase: {  // (case xxx).name
      auto *casePropExpr = static_cast<CaseExpression *>(lhs);
      if (casePropExpr->hasCondition()) {
        casePropExpr->condition()->accept(this);
      }
      if (casePropExpr->hasDefault()) {
        casePropExpr->defaultResult()->accept(this);
      }
      for (const auto &whenThen : casePropExpr->cases()) {
        whenThen.when->accept(this);
        whenThen.then->accept(this);
      }
      break;
    }
    case Expression::Kind::kSubscript: {  // $-.e[0].name
      auto *subscriptExpr = static_cast<SubscriptExpression *>(lhs);
      auto *subLeftExpr = subscriptExpr->left();
      auto kind = subLeftExpr->kind();
      if (kind == Expression::Kind::kInputProperty || kind == Expression::Kind::kVarProperty) {
        auto *propExpr = static_cast<PropertyExpression *>(subLeftExpr);
        auto &edgeAlias = propExpr->prop();
        propsUsed_.insertEdgeProp(edgeAlias, unknownType_, propName);
      } else if (kind == Expression::Kind::kListComprehension) {
        //  match (src_v:player{name:"Manu Ginobili"})-[e*2]-(dst_v) return e[0].start_year
        auto *listExpr = static_cast<ListComprehensionExpression *>(subLeftExpr);
        auto *collectExpr = listExpr->collection();
        if (collectExpr->kind() == Expression::Kind::kInputProperty) {
          auto *inputPropExpr = static_cast<InputPropertyExpression *>(collectExpr);
          auto &aliasName = inputPropExpr->prop();
          propsUsed_.insertEdgeProp(aliasName, unknownType_, propName);
        }
      } else {
        // normal path
        lhs->accept(this);
        rhs->accept(this);
      }
      break;
    }
    case Expression::Kind::kFunctionCall: {
      auto *funCallExpr = static_cast<FunctionCallExpression *>(lhs);
      auto funName = funCallExpr->name();
      std::transform(funName.begin(), funName.end(), funName.begin(), ::tolower);
      if (funName != "properties") {
        // normal path
        lhs->accept(this);
        rhs->accept(this);
        break;
      }
      auto *argExpr = funCallExpr->args()->args().front();
      auto kind = argExpr->kind();
      switch (kind) {
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kInputProperty: {  // properties($e).name
          //  match (v) return properties(v).name
          auto *inputPropExpr = static_cast<InputPropertyExpression *>(argExpr);
          auto &aliasName = inputPropExpr->prop();
          propsUsed_.insertVertexProp(aliasName, unknownType_, propName);
          break;
        }
        case Expression::Kind::kCase: {  // properties(case xxx).name
          auto *casePropExpr = static_cast<CaseExpression *>(argExpr);
          if (casePropExpr->hasCondition()) {
            casePropExpr->condition()->accept(this);
          }
          if (casePropExpr->hasDefault()) {
            casePropExpr->defaultResult()->accept(this);
          }
          for (const auto &whenThen : casePropExpr->cases()) {
            whenThen.when->accept(this);
            whenThen.then->accept(this);
          }
          break;
        }
        case Expression::Kind::kSubscript: {  // properties($-.e[0]).name
          auto *subscriptExpr = static_cast<SubscriptExpression *>(argExpr);
          auto *subLeftExpr = subscriptExpr->left();
          auto leftKind = subLeftExpr->kind();
          if (leftKind == Expression::Kind::kInputProperty ||
              leftKind == Expression::Kind::kVarProperty) {
            //  match (v)-[e]->(b) return properties(e).start_year
            auto *propExpr = static_cast<PropertyExpression *>(subLeftExpr);
            auto &aliasName = propExpr->prop();
            propsUsed_.insertEdgeProp(aliasName, unknownType_, propName);
          } else if (leftKind == Expression::Kind::kListComprehension) {
            //  match (v)-[c*2]->(b) return properties(c[0]).start_year
            //  properties([e IN $-.c WHERE is_edge($e)][0]).start_year
            auto *listExpr = static_cast<ListComprehensionExpression *>(subLeftExpr);
            auto *collectExpr = listExpr->collection();
            if (collectExpr->kind() == Expression::Kind::kInputProperty) {
              auto *inputPropExpr = static_cast<InputPropertyExpression *>(collectExpr);
              auto &aliasName = inputPropExpr->prop();
              propsUsed_.insertEdgeProp(aliasName, unknownType_, propName);
            }
          } else {
            // normal path
            lhs->accept(this);
            rhs->accept(this);
          }
          break;
        }
        default:
          break;
      }
      break;
    }
    default:
      // normal path
      lhs->accept(this);
      rhs->accept(this);
      break;
  }
}

void PropertyTrackerVisitor::visit(FunctionCallExpression *expr) {
  // length function support `STRING` input too, so we can't ignore it directly
  // TODO add type info to variable to help optimize it.
  static const std::unordered_set<std::string> ignoreFuncs = {
      "src", "dst", "type", "typeid", "id", "rank" /*, "length"*/};

  auto funName = expr->name();
  std::transform(funName.begin(), funName.end(), funName.begin(), ::tolower);
  if (ignoreFuncs.find(funName) != ignoreFuncs.end()) {
    return;
  }

  for (auto *arg : expr->args()->args()) {
    arg->accept(this);
    if (!ok()) {
      break;
    }
  }
}

void PropertyTrackerVisitor::visit(AggregateExpression *expr) {
  auto funName = expr->name();
  std::transform(funName.begin(), funName.end(), funName.begin(), ::tolower);
  if (funName == "count") {
    auto kind = expr->arg()->kind();
    if (kind == Expression::Kind::kVarProperty || kind == Expression::Kind::kConstant ||
        kind == Expression::Kind::kInputProperty) {
      return;
    }
  }
  // count(v.player.age)
  expr->arg()->accept(this);
}

void PropertyTrackerVisitor::visit(DestPropertyExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(SourcePropertyExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeSrcIdExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeTypeExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeRankExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeDstIdExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(UUIDExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VariableExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VersionedVariableExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(LabelExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(LabelAttributeExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(ConstantExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(ColumnExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(VertexExpression *expr) {
  UNUSED(expr);
}

void PropertyTrackerVisitor::visit(EdgeExpression *expr) {
  UNUSED(expr);
}

}  // namespace graph
}  // namespace nebula
