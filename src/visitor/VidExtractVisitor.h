/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_VIDEXTRACTVISITOR_H_
#define VISITOR_VIDEXTRACTVISITOR_H_

#include <memory>

#include "common/expression/ExprVisitor.h"
#include "util/SchemaUtil.h"

namespace nebula {

class Expression;

namespace graph {

// Reverse Eval the id(V) IN [List] or id(V) == vid that suppose result is TRUE
// Keep the constraint :
//    Put any vid in result VidPattern.vids to id(V) make the expression evaluate to TRUE!(Ignore
//    not related expression component e.g. attribute expr, so the filter still need eval
//    when filter final result) and non any other vid make it eval to TRUE!
class VidExtractVisitor final : public ExprVisitor {
public:
    VidExtractVisitor() = default;

    struct VidPattern {
        enum class Special {
            kIgnore,
            kInUsed,
        } spec{Special::kIgnore};

        struct Vids {
            enum class Kind {
                kOtherSource,   // e.g. PropertiesSeek
                kIn,
                kNotIn,
            } kind{Kind::kOtherSource};
            List vids;
        };
        std::unordered_map<std::string, Vids> nodes;
    };

    VidPattern moveVidPattern() {
        return std::move(vidPattern_);
    }

    static VidPattern intersect(VidPattern &&left, VidPattern &&right);

    static VidPattern intersect(VidPattern &&left,
                                std::pair<std::string, VidPattern::Vids> &&right);

    static VidPattern intersect(std::pair<std::string, VidPattern::Vids> &&left,
                                std::pair<std::string, VidPattern::Vids> &&right);

    void visit(ConstantExpression *expr) override;
    void visit(UnaryExpression *expr) override;
    void visit(TypeCastingExpression *expr) override;
    void visit(LabelExpression *expr) override;
    void visit(LabelAttributeExpression *expr) override;
    // binary expression
    void visit(ArithmeticExpression *expr) override;
    void visit(RelationalExpression *expr) override;
    void visit(SubscriptExpression *expr) override;
    void visit(AttributeExpression *expr) override;
    void visit(LogicalExpression *expr) override;
    // function call
    void visit(FunctionCallExpression *expr) override;
    void visit(UUIDExpression *expr) override;
    // variable expression
    void visit(VariableExpression *expr) override;
    void visit(VersionedVariableExpression *expr) override;
    // container expression
    void visit(ListExpression *expr) override;
    void visit(SetExpression *expr) override;
    void visit(MapExpression *expr) override;
    // property Expression
    void visit(TagPropertyExpression *expr) override;
    void visit(EdgePropertyExpression *expr) override;
    void visit(InputPropertyExpression *expr) override;
    void visit(VariablePropertyExpression *expr) override;
    void visit(DestPropertyExpression *expr) override;
    void visit(SourcePropertyExpression *expr) override;
    void visit(EdgeSrcIdExpression *expr) override;
    void visit(EdgeTypeExpression *expr) override;
    void visit(EdgeRankExpression *expr) override;
    void visit(EdgeDstIdExpression *expr) override;
    // vertex/edge expression
    void visit(VertexExpression *expr) override;
    void visit(EdgeExpression *expr) override;
    // case expression
    void visit(CaseExpression *expr) override;
    // path build expression
    void visit(PathBuildExpression *expr) override;
    // column expression
    void visit(ColumnExpression *expr) override;
    void visit(ListComprehensionExpression *expr) override;
    void visit(AggregateExpression *expr) override;
    void visit(PredicateExpression *expr) override;
    void visit(ReduceExpression *expr) override;
    // subscript range expression
    void visit(SubscriptRangeExpression *expr) override;

private:
    void visitBinaryExpr(BinaryExpression *expr);

    VidPattern vidPattern_{};
};

std::ostream& operator<<(std::ostream &os, const VidExtractVisitor::VidPattern &vp);

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_VIDEXTRACTVISITOR_H_
