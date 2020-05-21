/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_FUNCTIONCALLEXPRESSION_H_
#define EXPRESSION_FUNCTIONCALLEXPRESSION_H_

#include "expression/Expression.h"

namespace nebula {
class ArgumentList final {
public:
    void addArgument(Expression* arg) {
        args_.emplace_back(arg);
    }

    auto args() {
        return std::move(args_);
    }

private:
    std::vector<std::unique_ptr<Expression>> args_;
};

class FunctionCallExpression final : public Expression {
public:
    FunctionCallExpression(std::string* name, ArgumentList* args)
        : Expression(Kind::kFunctionCall) {
        name_.reset(name);
        args_.reset(args);
    }

    Value eval() const override;

    std::string encode() const override {
        // TODO
        return "";
    }

    std::string decode() const override {
        // TODO
        return "";
    }

    std::string toString() const override {
        // TODO
        return "";
    }

private:
    std::unique_ptr<std::string> name_;
    std::unique_ptr<ArgumentList> args_;
};
}   // namespace nebula
#endif
