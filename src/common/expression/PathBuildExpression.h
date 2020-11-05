/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_PATHBUILDEXPRESSION_H_
#define COMMON_EXPRESSION_PATHBUILDEXPRESSION_H_

#include "common/datatypes/Edge.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/Expression.h"

namespace nebula {
class PathBuildExpression final : public Expression {
public:
    PathBuildExpression() : Expression(Kind::kPathBuild) {
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression &rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override;

    PathBuildExpression& add(std::unique_ptr<Expression> expr) {
        items_.emplace_back(std::move(expr));
        return *this;
    }

    void setItem(size_t index, std::unique_ptr<Expression> item) {
        DCHECK_LT(index, items_.size());
        items_[index] = std::move(item);
    }

    size_t size() const {
        return items_.size();
    }

    size_t length() const {
        return size() - 1;
    }

    const auto& items() const {
        return items_;
    }

private:
    void writeTo(Encoder &encoder) const override;

    void resetFrom(Decoder &decoder) override;

    bool getVertex(const Value& value, Vertex& vertex) const;

    bool getEdge(const Value& value, Step& step) const;

private:
    std::vector<std::unique_ptr<Expression>>    items_;
    Value                                       result_;
};
}  // namespace nebula
#endif  // COMMON_EXPRESSION_PATHBUILDEXPRESSION_H_
