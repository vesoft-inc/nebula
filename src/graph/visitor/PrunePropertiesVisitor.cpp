/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/PrunePropertiesVisitor.h"

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
  if (node->condition() != nullptr) {
    status_ = extractPropsFromExpr(node->condition());
    if (!status_.ok()) {
      return;
    }
  }

  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(Project *node) {
  if (node->columns()) {
    const auto &columns = node->columns()->columns();
    auto &colNames = node->colNames();
    std::vector<bool> aliasExists(colNames.size(), false);
    for (size_t i = 0; i < columns.size(); ++i) {
      aliasExists[i] = propsUsed_.hasAlias(colNames[i]);
    }
    for (size_t i = 0; i < columns.size(); ++i) {
      auto *col = DCHECK_NOTNULL(columns[i]);
      auto *expr = col->expr();
      auto &alias = colNames[i];
      // If the alias exists, try to rename alias
      if (aliasExists[i]) {
        if (expr->kind() == Expression::Kind::kInputProperty) {
          auto *inputPropExpr = static_cast<InputPropertyExpression *>(expr);
          auto &newAlias = inputPropExpr->prop();
          status_ = propsUsed_.update(alias, newAlias);
          if (!status_.ok()) {
            return;
          }
        } else if (expr->kind() == Expression::Kind::kVarProperty) {
          auto *varPropExpr = static_cast<VariablePropertyExpression *>(expr);
          auto &newAlias = varPropExpr->prop();
          status_ = propsUsed_.update(alias, newAlias);
          if (!status_.ok()) {
            return;
          }
        } else {  // eg. "PathBuild[$-.x,$-.__VAR_0,$-.y] AS p"
          // How to handle this case?
          propsUsed_.colsSet.erase(alias);
          status_ = extractPropsFromExpr(expr);
          if (!status_.ok()) {
            return;
          }
        }
      } else {
        // Otherwise, extract properties from the column expression
        status_ = extractPropsFromExpr(expr);
        if (!status_.ok()) {
          return;
        }
      }
    }
  }

  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(Aggregate *node) {
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
  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(Traverse *node) {
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

  auto *vertexProps = node->vertexProps();
  if (propsUsed_.colsSet.find(nodeAlias) == propsUsed_.colsSet.end() && vertexProps != nullptr) {
    auto it2 = propsUsed_.vertexPropsMap.find(nodeAlias);
    if (it2 == propsUsed_.vertexPropsMap.end()) {  // nodeAlias is not used
      node->setVertexProps(nullptr);
    } else {
      auto prunedVertexProps = std::make_unique<std::vector<VertexProp>>();
      auto &usedVertexProps = it2->second;
      if (usedVertexProps.empty()) {
        node->setVertexProps(nullptr);
        status_ = depsPruneProperties(node->dependencies());
        return;
      }
      prunedVertexProps->reserve(usedVertexProps.size());
      for (auto &vertexProp : *vertexProps) {
        auto tagId = vertexProp.tag_ref().value();
        auto &props = vertexProp.props_ref().value();
        auto it3 = usedVertexProps.find(tagId);
        if (it3 != usedVertexProps.end()) {
          auto &usedProps = it3->second;
          VertexProp newVProp;
          newVProp.tag_ref() = tagId;
          std::vector<std::string> newProps;
          for (auto &prop : props) {
            if (usedProps.find(prop) != usedProps.end()) {
              newProps.emplace_back(prop);
            }
          }
          newVProp.props_ref() = std::move(newProps);
          prunedVertexProps->emplace_back(std::move(newVProp));
        }
      }
      node->setVertexProps(std::move(prunedVertexProps));
    }
  }

  static const std::unordered_set<std::string> reservedEdgeProps = {
      nebula::kSrc, nebula::kType, nebula::kRank, nebula::kDst};
  auto *edgeProps = node->edgeProps();
  if (propsUsed_.colsSet.find(edgeAlias) == propsUsed_.colsSet.end() && edgeProps != nullptr) {
    auto prunedEdgeProps = std::make_unique<std::vector<EdgeProp>>();
    prunedEdgeProps->reserve(edgeProps->size());
    auto it2 = propsUsed_.edgePropsMap.find(edgeAlias);

    for (auto &edgeProp : *edgeProps) {
      auto edgeType = edgeProp.type_ref().value();
      auto &props = edgeProp.props_ref().value();
      EdgeProp newEProp;
      newEProp.type_ref() = edgeType;
      std::vector<std::string> newProps{reservedEdgeProps.begin(), reservedEdgeProps.end()};
      std::unordered_set<std::string> usedProps;
      if (it2 != propsUsed_.edgePropsMap.end()) {
        auto &usedEdgeProps = it2->second;
        auto it3 = usedEdgeProps.find(std::abs(edgeType));
        if (it3 != usedEdgeProps.end()) {
          usedProps = {it3->second.begin(), it3->second.end()};
        }
        static const int kUnknownEdgeType = 0;
        auto it4 = usedEdgeProps.find(kUnknownEdgeType);
        if (it4 != usedEdgeProps.end()) {
          usedProps.insert(it4->second.begin(), it4->second.end());
        }
      }
      for (auto &prop : props) {
        if (reservedEdgeProps.find(prop) == reservedEdgeProps.end() &&
            usedProps.find(prop) != usedProps.end()) {
          newProps.emplace_back(prop);
        }
      }
      newEProp.props_ref() = std::move(newProps);
      prunedEdgeProps->emplace_back(std::move(newEProp));
    }
    node->setEdgeProps(std::move(prunedEdgeProps));
  }

  status_ = depsPruneProperties(node->dependencies());
}

// AppendVertices should be deleted when no properties it pulls are used by the parent node.
void PrunePropertiesVisitor::visit(AppendVertices *node) {
  auto &colNames = node->colNames();
  DCHECK(!colNames.empty());
  auto &nodeAlias = colNames.back();
  auto it = propsUsed_.colsSet.find(nodeAlias);
  if (it != propsUsed_.colsSet.end()) {  // All properties are used
    // propsUsed_.colsSet.erase(it);
    status_ = depsPruneProperties(node->dependencies());
    return;
  }

  if (node->vFilter() != nullptr) {
    status_ = extractPropsFromExpr(node->vFilter(), nodeAlias);
    if (!status_.ok()) {
      return;
    }
  }
  auto *vertexProps = node->props();
  if (vertexProps != nullptr) {
    auto prunedVertexProps = std::make_unique<std::vector<VertexProp>>();
    auto it2 = propsUsed_.vertexPropsMap.find(nodeAlias);
    if (it2 != propsUsed_.vertexPropsMap.end()) {
      auto &usedVertexProps = it2->second;
      if (usedVertexProps.empty()) {
        node->markDeleted();
        status_ = depsPruneProperties(node->dependencies());
        return;
      }
      prunedVertexProps->reserve(usedVertexProps.size());
      for (auto &vertexProp : *vertexProps) {
        auto tagId = vertexProp.tag_ref().value();
        auto &props = vertexProp.props_ref().value();
        auto it3 = usedVertexProps.find(tagId);
        if (it3 != usedVertexProps.end()) {
          auto &usedProps = it3->second;
          VertexProp newVProp;
          newVProp.tag_ref() = tagId;
          std::vector<std::string> newProps;
          for (auto &prop : props) {
            if (usedProps.find(prop) != usedProps.end()) {
              newProps.emplace_back(prop);
            }
          }
          newVProp.props_ref() = std::move(newProps);
          prunedVertexProps->emplace_back(std::move(newVProp));
        }
      }
    } else {
      node->markDeleted();
      status_ = depsPruneProperties(node->dependencies());
      return;
    }
    node->setVertexProps(std::move(prunedVertexProps));
  }

  status_ = depsPruneProperties(node->dependencies());
}

void PrunePropertiesVisitor::visit(BiJoin *node) {
  for (auto *hashKey : node->hashKeys()) {
    status_ = extractPropsFromExpr(hashKey);
    if (!status_.ok()) {
      return;
    }
  }
  for (auto *probeKey : node->probeKeys()) {
    status_ = extractPropsFromExpr(probeKey);
    if (!status_.ok()) {
      return;
    }
  }
  status_ = depsPruneProperties(node->dependencies());
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
