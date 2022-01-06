/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/DeducePropsVisitor.h"

#include <sstream>

#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

// Expression properties
void ExpressionProps::insertVarProp(const std::string &outputVar, folly::StringPiece prop) {
  auto &props = varProps_[outputVar];
  props.emplace(prop);
}

void ExpressionProps::insertInputProp(folly::StringPiece prop) {
  inputProps_.emplace(prop);
}

void ExpressionProps::insertSrcTagProp(TagID tagId, folly::StringPiece prop) {
  auto &props = srcTagProps_[tagId];
  props.emplace(prop);
}

void ExpressionProps::insertDstTagProp(TagID tagId, folly::StringPiece prop) {
  auto &props = dstTagProps_[tagId];
  props.emplace(prop);
}

void ExpressionProps::insertEdgeProp(EdgeType edgeType, folly::StringPiece prop) {
  auto &props = edgeProps_[edgeType];
  props.emplace(prop);
}

void ExpressionProps::insertTagNameIds(const std::string &name, TagID tagId) {
  tagNameIds_.emplace(name, tagId);
}

void ExpressionProps::insertTagProp(TagID tagId, folly::StringPiece prop) {
  auto &props = tagProps_[tagId];
  props.emplace(prop);
}

bool ExpressionProps::isSubsetOfInput(const std::set<folly::StringPiece> &props) {
  for (auto &prop : props) {
    if (inputProps_.find(prop) == inputProps_.end()) {
      return false;
    }
  }
  return true;
}

bool ExpressionProps::isSubsetOfVar(const VarPropMap &props) {
  for (auto &iter : props) {
    if (varProps_.find(iter.first) == varProps_.end()) {
      return false;
    }
    for (auto &prop : iter.second) {
      if (varProps_[iter.first].find(prop) == varProps_[iter.first].end()) {
        return false;
      }
    }
  }
  return true;
}

void ExpressionProps::unionProps(ExpressionProps exprProps) {
  if (!exprProps.inputProps().empty()) {
    inputProps_.insert(std::make_move_iterator(exprProps.inputProps().begin()),
                       std::make_move_iterator(exprProps.inputProps().end()));
  }
  if (!exprProps.srcTagProps().empty()) {
    for (auto &iter : exprProps.srcTagProps()) {
      srcTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                      std::make_move_iterator(iter.second.end()));
    }
  }
  if (!exprProps.dstTagProps().empty()) {
    for (auto &iter : exprProps.dstTagProps()) {
      dstTagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                      std::make_move_iterator(iter.second.end()));
    }
  }
  if (!exprProps.tagProps().empty()) {
    for (auto &iter : exprProps.tagProps()) {
      tagProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                   std::make_move_iterator(iter.second.end()));
    }
  }
  if (!exprProps.varProps().empty()) {
    for (auto &iter : exprProps.varProps()) {
      varProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                   std::make_move_iterator(iter.second.end()));
    }
  }
  if (!exprProps.edgeProps().empty()) {
    for (auto &iter : exprProps.edgeProps()) {
      edgeProps_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                    std::make_move_iterator(iter.second.end()));
    }
  }
}

// visitor
DeducePropsVisitor::DeducePropsVisitor(QueryContext *qctx,
                                       GraphSpaceID space,
                                       ExpressionProps *exprProps,
                                       std::set<std::string> *userDefinedVarNameList,
                                       std::vector<TagID> *tagIds,
                                       std::vector<EdgeType> *edgeTypes)
    : qctx_(qctx),
      space_(space),
      exprProps_(exprProps),
      userDefinedVarNameList_(userDefinedVarNameList),
      tagIds_(tagIds),
      edgeTypes_(edgeTypes) {
  DCHECK(qctx != nullptr);
  DCHECK(exprProps != nullptr);
  DCHECK(userDefinedVarNameList != nullptr);
}

void DeducePropsVisitor::visit(EdgePropertyExpression *expr) {
  visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(TagPropertyExpression *expr) {
  auto status = qctx_->schemaMng()->toTagID(space_, expr->sym());
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  exprProps_->insertTagNameIds(expr->sym(), status.value());
  exprProps_->insertTagProp(status.value(), expr->prop());
}

void DeducePropsVisitor::visit(LabelTagPropertyExpression *expr) {
  auto status = qctx_->schemaMng()->toTagID(space_, expr->sym());
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  exprProps_->insertTagNameIds(expr->sym(), status.value());
  exprProps_->insertTagProp(status.value(), expr->prop());
}

void DeducePropsVisitor::visit(InputPropertyExpression *expr) {
  exprProps_->insertInputProp(expr->prop());
}

void DeducePropsVisitor::visit(VariablePropertyExpression *expr) {
  exprProps_->insertVarProp(expr->sym(), expr->prop());
  userDefinedVarNameList_->emplace(expr->sym());
}

void DeducePropsVisitor::visit(DestPropertyExpression *expr) {
  auto status = qctx_->schemaMng()->toTagID(space_, expr->sym());
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  exprProps_->insertDstTagProp(std::move(status).value(), expr->prop());
}

void DeducePropsVisitor::visit(SourcePropertyExpression *expr) {
  auto status = qctx_->schemaMng()->toTagID(space_, expr->sym());
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  exprProps_->insertSrcTagProp(std::move(status).value(), expr->prop());
}

void DeducePropsVisitor::visit(EdgeSrcIdExpression *expr) {
  visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeTypeExpression *expr) {
  visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeRankExpression *expr) {
  visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(EdgeDstIdExpression *expr) {
  visitEdgePropExpr(expr);
}

void DeducePropsVisitor::visit(UUIDExpression *expr) {
  reportError(expr);
}

void DeducePropsVisitor::visit(VariableExpression *expr) {
  UNUSED(expr);
}

void DeducePropsVisitor::visit(VersionedVariableExpression *expr) {
  reportError(expr);
}

void DeducePropsVisitor::visit(LabelExpression *expr) {
  reportError(expr);
}

void DeducePropsVisitor::visit(LabelAttributeExpression *expr) {
  reportError(expr);
}

void DeducePropsVisitor::visit(ConstantExpression *expr) {
  UNUSED(expr);
}

void DeducePropsVisitor::visit(ColumnExpression *expr) {
  UNUSED(expr);
}

void DeducePropsVisitor::visit(VertexExpression *expr) {
  if (tagIds_ == nullptr) {
    UNUSED(expr);
    return;
  }
  const auto &colName = expr->name();
  for (const auto &tagID : *tagIds_) {
    const auto &tagSchema = qctx_->schemaMng()->getTagSchema(space_, tagID);
    if (colName == "$^") {
      exprProps_->insertSrcTagProp(tagID, nebula::kTag);
      for (size_t i = 0; i < tagSchema->getNumFields(); ++i) {
        exprProps_->insertSrcTagProp(tagID, tagSchema->getFieldName(i));
      }
    } else if (colName == "$$") {
      exprProps_->insertDstTagProp(tagID, nebula::kTag);
      for (size_t i = 0; i < tagSchema->getNumFields(); ++i) {
        exprProps_->insertDstTagProp(tagID, tagSchema->getFieldName(i));
      }
    } else {
      exprProps_->insertTagProp(tagID, nebula::kTag);
      for (size_t i = 0; i < tagSchema->getNumFields(); ++i) {
        exprProps_->insertTagProp(tagID, tagSchema->getFieldName(i));
      }
    }
  }
}

void DeducePropsVisitor::visit(EdgeExpression *expr) {
  if (edgeTypes_ == nullptr) {
    UNUSED(expr);
    return;
  }
  for (const auto &edgeType : *edgeTypes_) {
    const auto &edgeSchema = qctx_->schemaMng()->getEdgeSchema(space_, std::abs(edgeType));
    exprProps_->insertEdgeProp(edgeType, kType);
    exprProps_->insertEdgeProp(edgeType, kSrc);
    exprProps_->insertEdgeProp(edgeType, kDst);
    exprProps_->insertEdgeProp(edgeType, kRank);
    for (size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
      exprProps_->insertEdgeProp(edgeType, edgeSchema->getFieldName(i));
    }
  }
}

void DeducePropsVisitor::visitEdgePropExpr(PropertyExpression *expr) {
  const auto &edgeName = expr->sym();
  if (edgeName == "*") {
    for (const auto &edgeType : *edgeTypes_) {
      exprProps_->insertEdgeProp(edgeType, expr->prop());
    }
    return;
  }
  auto status = qctx_->schemaMng()->toEdgeType(space_, edgeName);
  if (!status.ok()) {
    status_ = std::move(status).status();
    return;
  }
  exprProps_->insertEdgeProp(status.value(), expr->prop());
}

void DeducePropsVisitor::reportError(const Expression *expr) {
  std::stringstream ss;
  ss << "Not supported expression `" << expr->toString() << "' for props deduction.";
  status_ = Status::SemanticError(ss.str());
}

}  // namespace graph
}  // namespace nebula
