/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/expression/VariableExpression.h"
#include "parser/GQLParser.h"

namespace nebula {


class ExpressionParsingTest : public ::testing::Test {
protected:
    template <typename T, typename...Args>
    static T* make(Args&&...args) {
        return new T(std::forward<Args>(args)...);
    }

    const Expression* parse(std::string expr) {
        std::string query = "GO FROM '1' OVER * WHERE " + expr;
        GQLParser parser;
        auto result = parser.parse(std::move(query));
        CHECK(result.ok()) << result.status();
        stmt_ = std::move(result).value();
        auto *seq = static_cast<SequentialSentences*>(stmt_.get());
        auto *go = static_cast<GoSentence*>(seq->sentences()[0]);
        expr_ = go->whereClause()->filter();
        return expr_;
    }

    void add(std::string expr, Expression *ast) {
        items_.emplace_back(std::move(expr), ast);
    }

    void run() {
        if (items_.empty()) {
            return;
        }

        // Combine expressions into a list expression
        std::string buf;
        buf.reserve(4096);
        buf += '[';
        for (auto &item : items_) {
            buf += item.first;
            buf += ',';
        }
        buf.resize(buf.size() - 1);
        buf += ']';

        // Parse and get the expression list
        auto *list = static_cast<const ListExpression*>(parse(buf));
        auto &items = list->items();
        ASSERT_EQ(items_.size(), items.size());

        // verify
        for (auto i = 0u; i < items.size(); i++) {
            auto *parsed = items[i].get();
            auto *expected = items_[i].second.get();
            ASSERT_EQ(*parsed, *expected) << "Expression: " << items_[i].first
                                          << ", Expected: " << expected->toString()
                                          << ", Actual:   " << parsed->toString();
        }
    }

protected:
    using Kind = Expression::Kind;
    using Item = std::pair<std::string, std::unique_ptr<Expression>>;
    std::vector<Item>                       items_;
    std::unique_ptr<Sentence>               stmt_;
    const Expression                       *expr_;
};


TEST_F(ExpressionParsingTest, Associativity) {
    Expression * ast = nullptr;

    ast = make<ArithmeticExpression>(Kind::kAdd,
                                     make<ArithmeticExpression>(Kind::kAdd,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 + 2 + 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ArithmeticExpression>(Kind::kMinus,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 - 2 - 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ArithmeticExpression>(Kind::kAdd,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 + 2 - 3", ast);

    ast = make<ArithmeticExpression>(Kind::kAdd,
                                     make<ArithmeticExpression>(Kind::kMinus,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 - 2 + 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMultiply,
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 * 3", ast);

    ast = make<ArithmeticExpression>(Kind::kDivision,
                                     make<ArithmeticExpression>(Kind::kDivision,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 / 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMod,
                                     make<ArithmeticExpression>(Kind::kMod,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 % 3", ast);

    ast = make<ArithmeticExpression>(Kind::kDivision,
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 / 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMod,
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 % 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMultiply,
                                     make<ArithmeticExpression>(Kind::kDivision,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 * 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMod,
                                     make<ArithmeticExpression>(Kind::kDivision,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 % 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMultiply,
                                     make<ArithmeticExpression>(Kind::kMod,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 * 3", ast);

    ast = make<ArithmeticExpression>(Kind::kDivision,
                                     make<ArithmeticExpression>(Kind::kMod,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 / 3", ast);

    ast = make<RelationalExpression>(Kind::kRelEQ,
                                     make<RelationalExpression>(Kind::kRelEQ,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 == 2 == 3", ast);

    ast = make<RelationalExpression>(Kind::kRelNE,
                                     make<RelationalExpression>(Kind::kRelNE,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 != 2 != 3", ast);

    ast = make<RelationalExpression>(Kind::kRelLT,
                                     make<RelationalExpression>(Kind::kRelLT,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 < 2 < 3", ast);

    ast = make<RelationalExpression>(Kind::kRelLE,
                                     make<RelationalExpression>(Kind::kRelLE,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 <= 2 <= 3", ast);

    ast = make<RelationalExpression>(Kind::kRelGT,
                                     make<RelationalExpression>(Kind::kRelGT,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 > 2 > 3", ast);

    ast = make<RelationalExpression>(Kind::kRelGE,
                                     make<RelationalExpression>(Kind::kRelGE,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 >= 2 >= 3", ast);

    ast = make<RelationalExpression>(Kind::kRelIn,
                                     make<RelationalExpression>(Kind::kRelIn,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a IN b IN c", ast);

    ast = make<RelationalExpression>(Kind::kRelNotIn,
                                     make<RelationalExpression>(Kind::kRelNotIn,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a NOT IN b NOT IN c", ast);

    ast = make<RelationalExpression>(Kind::kContains,
                                     make<RelationalExpression>(Kind::kContains,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a CONTAINS b CONTAINS c", ast);

    /*
    ast = make<RelationalExpression>(Kind::kRelNotContains,
                                     make<RelationalExpression>(Kind::kRelNotContains,
                                        make<LabelExpression>("a"),
                                        make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("1 NOT CONTAINS 2 NOT CONTAINS 3", ast);
    */
    ast = make<RelationalExpression>(Kind::kStartsWith,
                                     make<RelationalExpression>(Kind::kStartsWith,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a STARTS WITH b STARTS WITH c", ast);

    ast = make<RelationalExpression>(Kind::kEndsWith,
                                     make<RelationalExpression>(Kind::kEndsWith,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a ENDS WITH b ENDS WITH c", ast);

    ast = make<SubscriptExpression>(make<SubscriptExpression>(
                                        make<LabelExpression>("a"),
                                        make<LabelExpression>("b")),
                                    make<LabelExpression>("c"));
    add("a[b][c]", ast);

    ast = make<AttributeExpression>(make<LabelAttributeExpression>(
                                        make<LabelExpression>("a"),
                                        make<ConstantExpression>("b")),
                                    make<ConstantExpression>("c"));
    add("a.b.c", ast);

    ast = make<LogicalExpression>(Kind::kLogicalAnd,
                                  make<LogicalExpression>(Kind::kLogicalAnd,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a AND b AND c", ast);

    ast = make<LogicalExpression>(Kind::kLogicalAnd,
                                  make<LogicalExpression>(Kind::kLogicalAnd,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a AND b AND c", ast);

    ast = make<LogicalExpression>(Kind::kLogicalOr,
                                  make<LogicalExpression>(Kind::kLogicalOr,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a OR b OR c", ast);

    ast = make<LogicalExpression>(Kind::kLogicalOr,
                                  make<LogicalExpression>(Kind::kLogicalOr,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a OR b OR c", ast);

    ast = make<LogicalExpression>(Kind::kLogicalXor,
                                  make<LogicalExpression>(Kind::kLogicalXor,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a XOR b XOR c", ast);

    ast = make<TypeCastingExpression>(Value::Type::INT,
                                      make<TypeCastingExpression>(Value::Type::FLOAT,
                                          make<ConstantExpression>(3.14)));
    add("(int)(double)3.14", ast);

    ast = make<UnaryExpression>(Kind::kUnaryNot,
                                make<UnaryExpression>(Kind::kUnaryNot,
                                    make<ConstantExpression>(false)));
    add("!!false", ast);

    auto cases = new CaseList();
    cases->add(make<ConstantExpression>(3), make<ConstantExpression>(4));
    ast = make<CaseExpression>(cases);
    static_cast<CaseExpression*>(ast)->setCondition(make<LabelExpression>("a"));
    auto cases2 = new CaseList();
    cases2->add(make<ConstantExpression>(5), make<ConstantExpression>(6));
    auto ast2 = make<CaseExpression>(cases2);
    ast2->setCondition(make<LabelExpression>("b"));
    ast2->setDefault(make<ConstantExpression>(7));
    static_cast<CaseExpression*>(ast)->setDefault(ast2);
    add("CASE a WHEN 3 THEN 4 ELSE CASE b WHEN 5 THEN 6 ELSE 7 END END", ast);

    run();
}


TEST_F(ExpressionParsingTest, Precedence) {
    Expression *ast = nullptr;

    ast = make<ArithmeticExpression>(Kind::kAdd,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 * 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 * 3", ast);

    ast = make<ArithmeticExpression>(Kind::kAdd,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kDivision,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 / 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kDivision,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 / 3", ast);

    ast = make<ArithmeticExpression>(Kind::kAdd,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kMod,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 % 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ConstantExpression>(1),
                                     make<ArithmeticExpression>(Kind::kMod,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 % 3", ast);

    ast = make<RelationalExpression>(Kind::kRelLT,
                                     make<ArithmeticExpression>(Kind::kAdd,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ArithmeticExpression>(Kind::kAdd,
                                         make<ConstantExpression>(3),
                                         make<ConstantExpression>(4)));
    add("1 + 2 < 3 + 4", ast);

    ast = make<LogicalExpression>(Kind::kLogicalOr,
                                  make<UnaryExpression>(Kind::kUnaryPlus,
                                      make<ConstantExpression>(1)),
                                  make<LogicalExpression>(Kind::kLogicalAnd,
                                      make<RelationalExpression>(Kind::kRelNE,
                                          make<ConstantExpression>(1),
                                          make<ArithmeticExpression>(Kind::kMinus,
                                              make<ConstantExpression>(2),
                                              make<ConstantExpression>(1))),
                                      make<RelationalExpression>(Kind::kRelEQ,
                                          make<ConstantExpression>(3),
                                          make<ConstantExpression>(4))));
    add("+1 OR 1 != 2 - 1 AND 3 == 4", ast);

    ast = make<UnaryExpression>(Kind::kUnaryNot,
            make<RelationalExpression>(Kind::kRelLT,
                make<ArithmeticExpression>(Kind::kAdd,
                    make<ConstantExpression>(1),
                    make<ConstantExpression>(2)),
                make<ConstantExpression>(3)));
    add("NOT 1 + 2 < 3", ast);

    ast = make<LogicalExpression>(Kind::kLogicalAnd,
            make<UnaryExpression>(Kind::kUnaryNot,
                make<RelationalExpression>(Kind::kRelLT,
                    make<ArithmeticExpression>(Kind::kAdd,
                        make<ConstantExpression>(1),
                        make<ConstantExpression>(2)),
                    make<ConstantExpression>(3))),
            make<UnaryExpression>(Kind::kUnaryNot,
                make<RelationalExpression>(Kind::kRelLT,
                    make<ArithmeticExpression>(Kind::kAdd,
                        make<ConstantExpression>(1),
                        make<ConstantExpression>(2)),
                    make<ConstantExpression>(3))));
    add("NOT 1 + 2 < 3 AND NOT 1 + 2 < 3", ast);

    ast = make<ArithmeticExpression>(Kind::kMinus,
                                     make<ArithmeticExpression>(Kind::kMultiply,
                                         make<UnaryExpression>(Kind::kUnaryNot,
                                             make<ConstantExpression>(true)),
                                         make<UnaryExpression>(Kind::kUnaryNegate,
                                             make<ConstantExpression>(1))),
                                     make<UnaryExpression>(Kind::kUnaryNot,
                                         make<UnaryExpression>(Kind::kUnaryNot,
                                             make<ConstantExpression>(false))));
    add("!true * -1 - NOT NOT false", ast);

    ast = make<AttributeExpression>(
            make<SubscriptExpression>(
                make<SubscriptExpression>(
                    make<VariableExpression>("var"),
                    make<ConstantExpression>(0)),
                make<ConstantExpression>("1")),
            make<ConstantExpression>("m"));
    add("$var[0]['1'].m", ast);

    ast = make<LogicalExpression>(Kind::kLogicalXor,
                                  make<UnaryExpression>(Kind::kUnaryNot,
                                      make<ArithmeticExpression>(Kind::kMultiply,
                                          make<AttributeExpression>(
                                              make<LabelAttributeExpression>(
                                                  make<LabelExpression>("a"),
                                                  make<ConstantExpression>("b")),
                                              make<ConstantExpression>("c")),
                                      make<SubscriptExpression>(
                                          make<SubscriptExpression>(
                                              make<AttributeExpression>(
                                                  make<VariablePropertyExpression>("var", "p"),
                                                  make<ConstantExpression>("q")),
                                              make<LabelExpression>("r")),
                                          make<LabelExpression>("s")))),
                                  make<RelationalExpression>(Kind::kRelIn,
                                      make<InputPropertyExpression>("m"),
                                      make<ListExpression>(
                                          &(*(new ExpressionList))
                                          .add(make<ConstantExpression>(1))
                                          .add(make<ConstantExpression>(2))
                                          .add(make<ConstantExpression>(3)))));
    add("NOT a.b.c * $var.p.q[r][s] XOR $-.m IN [1,2,3]", ast);

    run();
}

}   // namespace nebula
