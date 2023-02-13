/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/PrunePropertiesVisitor.h"
DECLARE_bool(optimize_appendvertices);
namespace nebula {
namespace graph {

PrunePropertiesVisitor::PrunePropertiesVisitor(PropertyTracker &propsUsed,
                                               graph::QueryContext *qctx,
                                               GraphSpaceID spaceID)
    : propsUsed_(propsUsed), qctx_(qctx), spaceID_(spaceID) {
  DCHECK(qctx_ != nullptr);
}

void PrunePropertiesVisitor::visit(PlanNode *node) {
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(Filter *node) {
  visitCurrent(node);  // Filter will use properties in filter expression
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(Filter *node) {
  if (node->condition() != nullptr) {
    status_ = extractPropsFromExpr(node->condition());
    if (!status_.ok()) {
      return;
    }
  }
}

void PrunePropertiesVisitor::visit(Project *node) {
  visitCurrent(node);  // Project won't use properties in column expression
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(Project *node) {
  // TODO won't use properties of not-root Project
  if (!node->columns()) {
    return;
  }
  const auto &columns = node->columns()->columns();
  auto &colNames = node->colNames();
  if (rootNode_) {
    for (auto i = 0u; i < columns.size(); ++i) {
      auto *col = DCHECK_NOTNULL(columns[i]);
      auto *expr = col->expr();
      status_ = extractPropsFromExpr(expr);
      if (!status_.ok()) {
        // Project a not exit tag, should not break other columns
        // e.g. Vertex tag {{"name", "string"}, {"age", "int64"}}
        //      Project "... RETURN v.name, v.xxx.yyy, v.player.age"
        //      v.xxx.yyy should not break v.player.age
        if (status_.isTagNotFound()) {
          continue;
        }
        return;
      }
    }
    rootNode_ = false;
    return;
  }
  DCHECK_EQ(columns.size(), colNames.size());
  for (auto i = 0u; i < columns.size() && i < colNames.size(); ++i) {
    auto *col = DCHECK_NOTNULL(columns[i]);
    auto *expr = col->expr();
    auto &alias = colNames[i];
    switch (expr->kind()) {
      case Expression::Kind::kVarProperty:
      case Expression::Kind::kInputProperty: {
        if (propsUsed_.hasAlias(alias)) {
          auto *propExpr = static_cast<PropertyExpression *>(expr);
          auto &newAlias = propExpr->prop();
          status_ = propsUsed_.update(alias, newAlias);
          if (!status_.ok()) {
            return;
          }
        }
        break;
      }
      // $-.e[0] as e
      // [e IN $-.e WHERE is_edge($e)] AS e
      // PathBuild[$-.v,$-.e,$-.v2] AS p
      case Expression::Kind::kSubscript:
      case Expression::Kind::kPathBuild:
      case Expression::Kind::kListComprehension: {
        if (propsUsed_.hasAlias(alias)) {
          status_ = extractPropsFromExpr(expr);
          if (!status_.ok()) {
            return;
          }
        }
        break;
      }
      default: {
        status_ = extractPropsFromExpr(expr);
        if (!status_.ok()) {
          return;
        }
      }
    }
  }
}

void PrunePropertiesVisitor::visit(Aggregate *node) {
  visitCurrent(node);
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(Aggregate *node) {
  if (rootNode_) {
    for (auto *groupKey : node->groupKeys()) {
      status_ = extractPropsFromExpr(groupKey);
      if (!status_.ok()) {
        return;
      }
    }
    for (auto *groupItem : node->groupItems()) {
      status_ = extractPropsFromExpr(groupItem);
      if (!status_.ok()) {
        return;
      }
    }
    rootNode_ = false;
    return;
  }
  for (auto *groupKey : node->groupKeys()) {
    if (groupKey->kind() == Expression::Kind::kVarProperty ||
        groupKey->kind() == Expression::Kind::kInputProperty ||
        groupKey->kind() == Expression::Kind::kConstant) {
      continue;
    }
    status_ = extractPropsFromExpr(groupKey);
    if (!status_.ok()) {
      return;
    }
  }
  for (auto *groupItem : node->groupItems()) {
    if (groupItem->kind() == Expression::Kind::kVarProperty ||
        groupItem->kind() == Expression::Kind::kInputProperty ||
        groupItem->kind() == Expression::Kind::kConstant) {
      continue;
    }
    status_ = extractPropsFromExpr(groupItem);
    if (!status_.ok()) {
      return;
    }
  }
}

void PrunePropertiesVisitor::visit(ScanEdges *node) {
  rootNode_ = false;
  pruneCurrent(node);
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::pruneCurrent(ScanEdges *node) {
  auto &colNames = node->colNames();
  DCHECK(!colNames.empty());
  auto &edgeAlias = colNames.back();
  auto it = propsUsed_.colsSet.find(edgeAlias);
  if (it != propsUsed_.colsSet.end()) {
    // all properties are used
    return;
  }
  auto *edgeProps = node->props();
  auto &edgePropsMap = propsUsed_.edgePropsMap;

  auto prunedEdgeProps = std::make_unique<std::vector<EdgeProp>>();
  prunedEdgeProps->reserve(edgeProps->size());
  auto edgeAliasIter = edgePropsMap.find(edgeAlias);

  for (auto &edgeProp : *edgeProps) {
    auto edgeType = edgeProp.type_ref().value();
    auto &props = edgeProp.props_ref().value();
    EdgeProp newEdgeProp;
    newEdgeProp.type_ref() = edgeType;
    if (edgeAliasIter == edgePropsMap.end()) {
      // only type, dst are used
      newEdgeProp.props_ref() = {nebula::kSrc, nebula::kDst, nebula::kType, nebula::kRank};
    } else {
      std::unordered_set<std::string> uniqueProps{
          nebula::kSrc, nebula::kDst, nebula::kType, nebula::kRank};
      std::vector<std::string> newProps;
      auto &usedEdgeProps = edgeAliasIter->second;
      auto edgeTypeIter = usedEdgeProps.find(std::abs(edgeType));
      if (edgeTypeIter != usedEdgeProps.end()) {
        uniqueProps.insert(edgeTypeIter->second.begin(), edgeTypeIter->second.end());
      }
      auto unknownEdgeIter = usedEdgeProps.find(unknownType_);
      if (unknownEdgeIter != usedEdgeProps.end()) {
        uniqueProps.insert(unknownEdgeIter->second.begin(), unknownEdgeIter->second.end());
      }
      for (auto &prop : props) {
        if (uniqueProps.find(prop) != uniqueProps.end()) {
          newProps.emplace_back(prop);
        }
      }
      newEdgeProp.props_ref() = std::move(newProps);
    }
    prunedEdgeProps->emplace_back(std::move(newEdgeProp));
  }
  node->setEdgeProps(std::move(prunedEdgeProps));
}

void PrunePropertiesVisitor::visit(Traverse *node) {
  rootNode_ = false;
  visitCurrent(node);
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(Traverse *node) {
  auto &colNames = node->colNames();
  DCHECK_GE(colNames.size(), 2);
  auto &nodeAlias = colNames[colNames.size() - 2];
  auto &edgeAlias = colNames.back();

  if (node->vFilter() != nullptr) {
    status_ = extractPropsFromExpr(node->vFilter(), nodeAlias);
    if (!status_.ok()) {
      return;
    }
  }

  if (node->eFilter() != nullptr) {
    status_ = extractPropsFromExpr(node->eFilter(), edgeAlias);
    if (!status_.ok()) {
      return;
    }
  }
  pruneCurrent(node);
}

void PrunePropertiesVisitor::pruneCurrent(Traverse *node) {
  auto &colNames = node->colNames();
  DCHECK_GE(colNames.size(), 2);
  auto &nodeAlias = colNames[colNames.size() - 2];
  auto &edgeAlias = colNames.back();
  auto *vertexProps = node->vertexProps();
  auto &colsSet = propsUsed_.colsSet;
  auto &vertexPropsMap = propsUsed_.vertexPropsMap;
  auto &edgePropsMap = propsUsed_.edgePropsMap;

  if (colsSet.find(nodeAlias) == colsSet.end()) {
    auto aliasIter = vertexPropsMap.find(nodeAlias);
    if (aliasIter == vertexPropsMap.end()) {
      node->setVertexProps(nullptr);
    } else {
      auto &usedVertexProps = aliasIter->second;
      if (usedVertexProps.empty()) {
        node->setVertexProps(nullptr);
      } else {
        auto unknownIter = usedVertexProps.find(unknownType_);
        auto prunedVertexProps = std::make_unique<std::vector<VertexProp>>();
        prunedVertexProps->reserve(usedVertexProps.size());
        for (auto &vertexProp : *vertexProps) {
          auto tagId = vertexProp.tag_ref().value();
          auto &props = vertexProp.props_ref().value();
          std::unordered_set<std::string> usedProps;
          if (unknownIter != usedVertexProps.end()) {
            usedProps.insert(unknownIter->second.begin(), unknownIter->second.end());
          }
          auto tagIter = usedVertexProps.find(tagId);
          if (tagIter != usedVertexProps.end()) {
            usedProps.insert(tagIter->second.begin(), tagIter->second.end());
          }
          if (usedProps.empty()) {
            continue;
          }
          std::vector<std::string> newProps;
          for (auto &prop : props) {
            if (usedProps.find(prop) != usedProps.end()) {
              newProps.emplace_back(prop);
            }
          }
          if (newProps.empty()) {
            continue;
          }
          VertexProp newVProp;
          newVProp.tag_ref() = tagId;
          newVProp.props_ref() = std::move(newProps);
          prunedVertexProps->emplace_back(std::move(newVProp));
        }
        node->setVertexProps(std::move(prunedVertexProps));
      }
    }
  }

  auto *edgeProps = node->edgeProps();
  if (colsSet.find(edgeAlias) != colsSet.end()) {
    // all edge properties are used
    return;
  }
  auto prunedEdgeProps = std::make_unique<std::vector<EdgeProp>>();
  prunedEdgeProps->reserve(edgeProps->size());
  auto edgeAliasIter = edgePropsMap.find(edgeAlias);

  for (auto &edgeProp : *edgeProps) {
    auto edgeType = edgeProp.type_ref().value();
    auto &props = edgeProp.props_ref().value();
    EdgeProp newEdgeProp;
    newEdgeProp.type_ref() = edgeType;
    if (edgeAliasIter == edgePropsMap.end()) {
      // only type, dst are used
      newEdgeProp.props_ref() = {nebula::kDst, nebula::kType, nebula::kRank};
    } else {
      std::unordered_set<std::string> uniqueProps{nebula::kDst, nebula::kType, nebula::kRank};
      std::vector<std::string> newProps;
      auto &usedEdgeProps = edgeAliasIter->second;
      auto edgeTypeIter = usedEdgeProps.find(std::abs(edgeType));
      if (edgeTypeIter != usedEdgeProps.end()) {
        uniqueProps.insert(edgeTypeIter->second.begin(), edgeTypeIter->second.end());
      }
      auto unknownEdgeIter = usedEdgeProps.find(unknownType_);
      if (unknownEdgeIter != usedEdgeProps.end()) {
        uniqueProps.insert(unknownEdgeIter->second.begin(), unknownEdgeIter->second.end());
      }
      for (auto &prop : props) {
        if (uniqueProps.find(prop) != uniqueProps.end()) {
          newProps.emplace_back(prop);
        }
      }
      newEdgeProp.props_ref() = std::move(newProps);
    }
    prunedEdgeProps->emplace_back(std::move(newEdgeProp));
  }
  node->setEdgeProps(std::move(prunedEdgeProps));
}

// AppendVertices should be deleted when no properties it pulls are used by the parent node.
void PrunePropertiesVisitor::visit(AppendVertices *node) {
  visitCurrent(node);
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(AppendVertices *node) {
  if (rootNode_) {
    rootNode_ = false;
    return;
  }
  auto &colNames = node->colNames();
  DCHECK(!colNames.empty());
  auto &nodeAlias = colNames.back();
  auto it = propsUsed_.colsSet.find(nodeAlias);
  if (it != propsUsed_.colsSet.end()) {
    // all properties are used
    return;
  }
  if (node->filter() != nullptr) {
    status_ = extractPropsFromExpr(node->filter(), nodeAlias);
    if (!status_.ok()) {
      return;
    }
  }
  if (node->vFilter() != nullptr) {
    status_ = extractPropsFromExpr(node->vFilter(), nodeAlias);
    if (!status_.ok()) {
      return;
    }
  }
  pruneCurrent(node);
}

void PrunePropertiesVisitor::pruneCurrent(AppendVertices *node) {
  auto &colNames = node->colNames();
  DCHECK(!colNames.empty());
  auto &nodeAlias = colNames.back();
  auto *vertexProps = node->props();
  if (vertexProps == nullptr) {
    return;
  }
  auto prunedVertexProps = std::make_unique<std::vector<VertexProp>>();
  auto &vertexPropsMap = propsUsed_.vertexPropsMap;
  auto aliasIter = vertexPropsMap.find(nodeAlias);
  if (aliasIter == vertexPropsMap.end()) {
    if (FLAGS_optimize_appendvertices) {
      node->setVertexProps(nullptr);
    } else {
      // only get _tag when props is nullptr
      for (auto &vertexProp : *vertexProps) {
        auto tagId = vertexProp.tag_ref().value();
        VertexProp newVProp;
        newVProp.tag_ref() = tagId;
        newVProp.props_ref() = {nebula::kTag};
        prunedVertexProps->emplace_back(std::move(newVProp));
      }
      node->setVertexProps(std::move(prunedVertexProps));
    }
    return;
  }
  auto &usedVertexProps = aliasIter->second;
  if (usedVertexProps.empty()) {
    if (FLAGS_optimize_appendvertices) {
      node->setVertexProps(nullptr);
    } else {
      // only get _tag when props is nullptr
      for (auto &vertexProp : *vertexProps) {
        auto tagId = vertexProp.tag_ref().value();
        VertexProp newVProp;
        newVProp.tag_ref() = tagId;
        newVProp.props_ref() = {nebula::kTag};
        prunedVertexProps->emplace_back(std::move(newVProp));
      }
      node->setVertexProps(std::move(prunedVertexProps));
    }
    return;
  }
  auto unknownIter = usedVertexProps.find(unknownType_);
  prunedVertexProps->reserve(usedVertexProps.size());
  for (auto &vertexProp : *vertexProps) {
    auto tagId = vertexProp.tag_ref().value();
    auto &props = vertexProp.props_ref().value();
    auto tagIter = usedVertexProps.find(tagId);
    std::unordered_set<std::string> usedProps;
    if (unknownIter != usedVertexProps.end()) {
      usedProps.insert(unknownIter->second.begin(), unknownIter->second.end());
    }
    if (tagIter != usedVertexProps.end()) {
      usedProps.insert(tagIter->second.begin(), tagIter->second.end());
    }
    if (usedProps.empty()) {
      continue;
    }
    std::vector<std::string> newProps;
    for (auto &prop : props) {
      if (usedProps.find(prop) != usedProps.end()) {
        newProps.emplace_back(prop);
      }
    }
    if (newProps.empty()) {
      continue;
    }
    VertexProp newVProp;
    newVProp.tag_ref() = tagId;
    newVProp.props_ref() = std::move(newProps);
    prunedVertexProps->emplace_back(std::move(newVProp));
  }
  node->setVertexProps(std::move(prunedVertexProps));
}

void PrunePropertiesVisitor::visit(HashJoin *node) {
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(CrossJoin *node) {
  status_ = pruneMultiBranch(node->dependencies());
}

void PrunePropertiesVisitor::visit(Union *node) {
  status_ = pruneMultiBranch(node->dependencies());
}

void PrunePropertiesVisitor::visit(Unwind *node) {
  visitCurrent(node);
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visitCurrent(Unwind *node) {
  const auto &alias = node->alias();
  auto expr = node->unwindExpr();
  auto kind = expr->kind();
  // unwind e.start_year as a
  if (propsUsed_.hasAlias(alias) ||
      (kind != Expression::Kind::kVarProperty && kind != Expression::Kind::kInputProperty)) {
    status_ = extractPropsFromExpr(node->unwindExpr());
    if (!status_.ok()) {
      return;
    }
  }
}

Status PrunePropertiesVisitor::pruneMultiBranch(std::vector<const PlanNode *> &dependencies) {
  DCHECK_EQ(dependencies.size(), 2);
  auto rightPropsUsed = propsUsed_;
  auto *leftDep = dependencies.front();
  const_cast<PlanNode *>(leftDep)->accept(this);
  if (!status_.ok()) {
    return status_;
  }
  rootNode_ = true;
  propsUsed_ = std::move(rightPropsUsed);
  auto *rightDep = dependencies.back();
  const_cast<PlanNode *>(rightDep)->accept(this);
  if (!status_.ok()) {
    return status_;
  }
  return Status::OK();
}

Status PrunePropertiesVisitor::depsPruneProperties(std::vector<const PlanNode *> &dependencies) {
  for (const auto *dep : dependencies) {
    const_cast<PlanNode *>(dep)->accept(this);
    if (!status_.ok()) {
      return status_;
    }
  }
  return Status::OK();
}

Status PrunePropertiesVisitor::extractPropsFromExpr(const Expression *expr,
                                                    const std::string &entityAlias) {
  PropertyTrackerVisitor visitor(qctx_, spaceID_, propsUsed_, entityAlias);
  const_cast<Expression *>(expr)->accept(&visitor);
  if (!visitor.ok()) {
    return visitor.status();
  }

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
