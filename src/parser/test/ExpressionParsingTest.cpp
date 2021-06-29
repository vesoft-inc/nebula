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

using graph::QueryContext;

class ExpressionParsingTest : public ::testing::Test {
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        pool = qctx_->objPool();
    }

protected:
    template <typename T, typename... Args>
    T *make(Args &&...args) {
        return T::make(pool, std::forward<Args>(args)...);
    }

    const Expression* parse(std::string expr) {
        std::string query = "GO FROM '1' OVER * WHERE " + expr;

        GQLParser parser(qctx_.get());
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
            auto *parsed = items[i];
            auto *expected = items_[i].second;
            ASSERT_EQ(*parsed, *expected) << "Expression: " << items_[i].first
                                          << ", Expected: " << expected->toString()
                                          << ", Actual:   " << parsed->toString();
        }
    }

protected:
    using Kind = Expression::Kind;
    using Item = std::pair<std::string, Expression*>;
    std::vector<Item>                       items_;
    std::unique_ptr<Sentence>               stmt_;
    const Expression                        *expr_;
    std::unique_ptr<QueryContext>           qctx_;
    ObjectPool                              *pool;
};

TEST_F(ExpressionParsingTest, Associativity) {
    Expression * ast = nullptr;

    ast = ArithmeticExpression::makeAdd(pool,
                                     ArithmeticExpression::makeAdd(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 + 2 + 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     ArithmeticExpression::makeMinus(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 - 2 - 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     ArithmeticExpression::makeAdd(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 + 2 - 3", ast);

    ast = ArithmeticExpression::makeAdd(pool,
                                     ArithmeticExpression::makeMinus(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 - 2 + 3", ast);

    ast = ArithmeticExpression::makeMultiply(pool,
                                     ArithmeticExpression::makeMultiply(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 * 3", ast);

    ast = ArithmeticExpression::makeDivision(pool,
                                     ArithmeticExpression::makeDivision(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 / 3", ast);

    ast = ArithmeticExpression::makeMod(pool,
                                     ArithmeticExpression::makeMod(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 % 3", ast);

    ast = ArithmeticExpression::makeDivision(pool,
                                     ArithmeticExpression::makeMultiply(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 / 3", ast);

    ast = ArithmeticExpression::makeMod(pool,
                                     ArithmeticExpression::makeMultiply(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 * 2 % 3", ast);

    ast = ArithmeticExpression::makeMultiply(pool,
                                     ArithmeticExpression::makeDivision(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 * 3", ast);

    ast = ArithmeticExpression::makeMod(pool,
                                     ArithmeticExpression::makeDivision(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 / 2 % 3", ast);

    ast = ArithmeticExpression::makeMultiply(pool,
                                     ArithmeticExpression::makeMod(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 * 3", ast);

    ast = ArithmeticExpression::makeDivision(pool,
                                     ArithmeticExpression::makeMod(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 % 2 / 3", ast);

    ast = RelationalExpression::makeEQ(pool,
                                     RelationalExpression::makeEQ(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 == 2 == 3", ast);

    ast = RelationalExpression::makeNE(pool,
                                     RelationalExpression::makeNE(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 != 2 != 3", ast);

    ast = RelationalExpression::makeLT(pool,
                                     RelationalExpression::makeLT(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 < 2 < 3", ast);

    ast = RelationalExpression::makeLE(pool,
                                     RelationalExpression::makeLE(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 <= 2 <= 3", ast);

    ast = RelationalExpression::makeGT(pool,
                                     RelationalExpression::makeGT(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 > 2 > 3", ast);

    ast = RelationalExpression::makeGE(pool,
                                     RelationalExpression::makeGE(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     make<ConstantExpression>(3));
    add("1 >= 2 >= 3", ast);

    ast = RelationalExpression::makeIn(pool,
                                     RelationalExpression::makeIn(pool,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a IN b IN c", ast);

    ast = RelationalExpression::makeNotIn(pool,
                                     RelationalExpression::makeNotIn(pool,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a NOT IN b NOT IN c", ast);

    ast = RelationalExpression::makeContains(pool,
                                     RelationalExpression::makeContains(pool,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a CONTAINS b CONTAINS c", ast);

    /*
    ast = RelationalExpression::makeNotContains(pool,
                                     RelationalExpression::makeNotContains(pool,
                                        make<LabelExpression>("a"),
                                        make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("1 NOT CONTAINS 2 NOT CONTAINS 3", ast);
    */
    ast = RelationalExpression::makeStartsWith(pool,
                                     RelationalExpression::makeStartsWith(pool,
                                         make<LabelExpression>("a"),
                                         make<LabelExpression>("b")),
                                     make<LabelExpression>("c"));
    add("a STARTS WITH b STARTS WITH c", ast);

    ast = RelationalExpression::makeEndsWith(pool,
                                     RelationalExpression::makeEndsWith(pool,
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

    ast = LogicalExpression::makeAnd(pool,
                                  LogicalExpression::makeAnd(pool,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a AND b AND c", ast);

    ast = LogicalExpression::makeAnd(pool,
                                  LogicalExpression::makeAnd(pool,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a AND b AND c", ast);

    ast = LogicalExpression::makeOr(pool,
                                  LogicalExpression::makeOr(pool,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a OR b OR c", ast);

    ast = LogicalExpression::makeOr(pool,
                                  LogicalExpression::makeOr(pool,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a OR b OR c", ast);

    ast = LogicalExpression::makeXor(pool,
                                  LogicalExpression::makeXor(pool,
                                      make<LabelExpression>("a"),
                                      make<LabelExpression>("b")),
                                  make<LabelExpression>("c"));
    add("a XOR b XOR c", ast);

    ast = make<TypeCastingExpression>(Value::Type::INT,
                                      make<TypeCastingExpression>(Value::Type::FLOAT,
                                          make<ConstantExpression>(3.14)));
    add("(int)(double)3.14", ast);

    ast = UnaryExpression::makeNot(pool,
                                UnaryExpression::makeNot(pool,
                                    make<ConstantExpression>(false)));
    add("!!false", ast);

    auto cases = CaseList::make(pool);
    cases->add(make<ConstantExpression>(3), make<ConstantExpression>(4));
    ast = make<CaseExpression>(cases);
    static_cast<CaseExpression*>(ast)->setCondition(make<LabelExpression>("a"));
    auto cases2 = CaseList::make(pool);
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

    ast = ArithmeticExpression::makeAdd(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeMultiply(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 * 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeMultiply(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 * 3", ast);

    ast = ArithmeticExpression::makeAdd(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeDivision(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 / 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeDivision(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 / 3", ast);

    ast = ArithmeticExpression::makeAdd(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeMod(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 + 2 % 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     make<ConstantExpression>(1),
                                     ArithmeticExpression::makeMod(pool,
                                         make<ConstantExpression>(2),
                                         make<ConstantExpression>(3)));
    add("1 - 2 % 3", ast);

    ast = RelationalExpression::makeLT(pool,
                                     ArithmeticExpression::makeAdd(pool,
                                         make<ConstantExpression>(1),
                                         make<ConstantExpression>(2)),
                                     ArithmeticExpression::makeAdd(pool,
                                         make<ConstantExpression>(3),
                                         make<ConstantExpression>(4)));
    add("1 + 2 < 3 + 4", ast);

    ast = LogicalExpression::makeOr(pool,
                                  UnaryExpression::makePlus(pool,
                                      make<ConstantExpression>(1)),
                                  LogicalExpression::makeAnd(pool,
                                      RelationalExpression::makeNE(pool,
                                          make<ConstantExpression>(1),
                                          ArithmeticExpression::makeMinus(pool,
                                              make<ConstantExpression>(2),
                                              make<ConstantExpression>(1))),
                                      RelationalExpression::makeEQ(pool,
                                          make<ConstantExpression>(3),
                                          make<ConstantExpression>(4))));
    add("+1 OR 1 != 2 - 1 AND 3 == 4", ast);

    ast = UnaryExpression::makeNot(pool,
            RelationalExpression::makeLT(pool,
                ArithmeticExpression::makeAdd(pool,
                    make<ConstantExpression>(1),
                    make<ConstantExpression>(2)),
                make<ConstantExpression>(3)));
    add("NOT 1 + 2 < 3", ast);

    ast = LogicalExpression::makeAnd(pool,
            UnaryExpression::makeNot(pool,
                RelationalExpression::makeLT(pool,
                    ArithmeticExpression::makeAdd(pool,
                        make<ConstantExpression>(1),
                        make<ConstantExpression>(2)),
                    make<ConstantExpression>(3))),
            UnaryExpression::makeNot(pool,
                RelationalExpression::makeLT(pool,
                    ArithmeticExpression::makeAdd(pool,
                        make<ConstantExpression>(1),
                        make<ConstantExpression>(2)),
                    make<ConstantExpression>(3))));
    add("NOT 1 + 2 < 3 AND NOT 1 + 2 < 3", ast);

    ast = ArithmeticExpression::makeMinus(pool,
                                     ArithmeticExpression::makeMultiply(pool,
                                         UnaryExpression::makeNot(pool,
                                             make<ConstantExpression>(true)),
                                         UnaryExpression::makeNegate(pool,
                                             make<ConstantExpression>(1))),
                                     UnaryExpression::makeNot(pool,
                                         UnaryExpression::makeNot(pool,
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

    ast = LogicalExpression::makeXor(pool,
                                  UnaryExpression::makeNot(pool,
                                      ArithmeticExpression::makeMultiply(pool,
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
                                  RelationalExpression::makeIn(pool,
                                      make<InputPropertyExpression>("m"),
                                      make<ListExpression>(
                                          &(*(ExpressionList::make(pool)))
                                          .add(make<ConstantExpression>(1))
                                          .add(make<ConstantExpression>(2))
                                          .add(make<ConstantExpression>(3)))));
    add("NOT a.b.c * $var.p.q[r][s] XOR $-.m IN [1,2,3]", ast);

    run();
}

}   // namespace nebula
