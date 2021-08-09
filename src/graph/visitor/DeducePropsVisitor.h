/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_DEDUCEPROPSVISITOR_H_
#define VISITOR_DEDUCEPROPSVISITOR_H_

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class QueryContext;

class ExpressionProps final {
public:
    using TagNameIds = std::unordered_map<std::string, TagID>;
    using TagIDPropsMap = std::unordered_map<TagID, std::set<folly::StringPiece>>;
    using EdgePropMap = std::unordered_map<EdgeType, std::set<folly::StringPiece>>;
    using VarPropMap = std::unordered_map<std::string, std::set<folly::StringPiece>>;

    ExpressionProps() = default;
    ~ExpressionProps() = default;

    const std::set<folly::StringPiece>& inputProps() const {
        return inputProps_;
    }
    const TagIDPropsMap& srcTagProps() const {
        return srcTagProps_;
    }
    const TagIDPropsMap& dstTagProps() const {
        return dstTagProps_;
    }
    const TagIDPropsMap& tagProps() const {
        return tagProps_;
    }
    const TagNameIds& tagNameIds() const {
        return tagNameIds_;
    }
    const EdgePropMap& edgeProps() const {
        return edgeProps_;
    }
    const VarPropMap& varProps() const {
        return varProps_;
    }

    bool hasInputVarProperty() const {
        return !inputProps_.empty() || !varProps_.empty();
    }

    bool hasSrcDstTagProperty() const {
        return !srcTagProps_.empty() || !dstTagProps_.empty();
    }

    bool isAllPropsEmpty() const {
        return inputProps_.empty() && varProps_.empty() &&
               srcTagProps_.empty() && edgeProps_.empty() &&
               dstTagProps_.empty();
    }

    void insertInputProp(folly::StringPiece prop);
    void insertVarProp(const std::string& outputVar, folly::StringPiece prop);
    void insertSrcTagProp(TagID tagId, folly::StringPiece prop);
    void insertDstTagProp(TagID tagId, folly::StringPiece prop);
    void insertEdgeProp(EdgeType edgeType, folly::StringPiece prop);
    void insertTagNameIds(const std::string &name, TagID tagId);
    void insertTagProp(TagID tagId, folly::StringPiece prop);
    bool isSubsetOfInput(const std::set<folly::StringPiece>& props);
    bool isSubsetOfVar(const VarPropMap& props);
    void unionProps(ExpressionProps exprProps);

private:
    std::set<folly::StringPiece> inputProps_;
    VarPropMap varProps_;
    TagIDPropsMap srcTagProps_;
    TagIDPropsMap dstTagProps_;
    EdgePropMap edgeProps_;
    TagIDPropsMap tagProps_;
    TagNameIds tagNameIds_;
};

class DeducePropsVisitor : public ExprVisitorImpl {
public:
    DeducePropsVisitor(QueryContext* qctx,
                       GraphSpaceID space,
                       ExpressionProps* exprProps,
                       std::set<std::string>* userDefinedVarNameList);

    bool ok() const override {
        return status_.ok();
    }

    const Status& status() const {
        return status_;
    }

private:
    using ExprVisitorImpl::visit;
    void visit(EdgePropertyExpression* expr) override;
    void visit(TagPropertyExpression* expr) override;
    void visit(InputPropertyExpression* expr) override;
    void visit(VariablePropertyExpression* expr) override;
    void visit(SourcePropertyExpression* expr) override;
    void visit(DestPropertyExpression* expr) override;
    void visit(EdgeSrcIdExpression* expr) override;
    void visit(EdgeTypeExpression* expr) override;
    void visit(EdgeRankExpression* expr) override;
    void visit(EdgeDstIdExpression* expr) override;
    void visit(UUIDExpression* expr) override;
    void visit(VariableExpression* expr) override;
    void visit(VersionedVariableExpression* expr) override;
    void visit(LabelExpression* expr) override;
    void visit(AttributeExpression* expr) override;
    void visit(LabelAttributeExpression* expr) override;
    void visit(ConstantExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;
    void visit(ColumnExpression* expr) override;

    void visitEdgePropExpr(PropertyExpression* expr);
    void reportError(const Expression* expr);

    QueryContext* qctx_{nullptr};
    GraphSpaceID space_;
    ExpressionProps* exprProps_{nullptr};
    std::set<std::string>* userDefinedVarNameList_{nullptr};
    Status status_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_DEDUCEPROPSVISITOR_H_
