/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_FUNCTIONCALLEXPRESSION_H_
#define COMMON_EXPRESSION_FUNCTIONCALLEXPRESSION_H_

#include <boost/algorithm/string.hpp>

#include "common/function/FunctionManager.h"
#include "common/expression/Expression.h"

namespace nebula {

class ArgumentList final {
public:
    static ArgumentList* make(ObjectPool* pool, size_t sz = 0) {
        DCHECK(!!pool);
        return pool->add(new ArgumentList(sz));
    }

    void addArgument(Expression* arg) {
        CHECK(!!arg);
        args_.emplace_back(arg);
    }

    const auto& args() const {
        return args_;
    }

    auto& args() {
        return args_;
    }

    size_t numArgs() const {
        return args_.size();
    }

    void setArg(size_t i, Expression* arg) {
        DCHECK_LT(i, numArgs());
        args_[i] = arg;
    }

    void setArgs(std::vector<Expression*> args) {
        args_ = args;
    }

    bool operator==(const ArgumentList& rhs) const;

private:
    ArgumentList() = default;
    explicit ArgumentList(size_t sz) {
        args_.reserve(sz);
    }

private:
    std::vector<Expression*> args_;
};

class FunctionCallExpression final : public Expression {
    friend class Expression;

public:
    FunctionCallExpression& operator=(const FunctionCallExpression& rhs) = delete;
    FunctionCallExpression& operator=(FunctionCallExpression&&) = delete;

    static FunctionCallExpression* make(ObjectPool* pool,
                                        const std::string& name = "",
                                        ArgumentList* args = nullptr) {
        DCHECK(!!pool);
        return args == nullptr
                   ? pool->add(new FunctionCallExpression(pool, name, ArgumentList::make(pool)))
                   : pool->add(new FunctionCallExpression(pool, name, args));
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto arguments = ArgumentList::make(pool_, args_->numArgs());
        for (auto& arg : args_->args()) {
            arguments->addArgument(arg->clone());
        }
        return FunctionCallExpression::make(pool_, name_, arguments);
    }

    const std::string& name() const {
        return name_;
    }

    bool isFunc(const std::string &name) const {
        return boost::iequals(name, name_);
    }

    const ArgumentList* args() const {
        return args_;
    }

    ArgumentList* args() {
        return args_;
    }

private:
    explicit FunctionCallExpression(ObjectPool* pool, const std::string& name, ArgumentList* args)
        : Expression(pool, Kind::kFunctionCall), name_(name), args_(args) {
        if (!name_.empty()) {
            auto funcResult = FunctionManager::get(name_, DCHECK_NOTNULL(args_)->numArgs());
            if (funcResult.ok()) {
                func_ = funcResult.value();
            }
        }
    }

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    std::string name_;
    ArgumentList* args_;

    // runtime cache
    Value result_;
    FunctionManager::Function func_;
};

}  // namespace nebula
#endif  // EXPRESSION_FUNCTIONCALLEXPRESSION_H_
