/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/FunctionCallExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

bool ArgumentList::operator==(const ArgumentList& rhs) const {
    if (args_.size() != rhs.args_.size()) {
        return false;
    }

    for (size_t i = 0; i < args_.size(); i++) {
        if (*(args_[i]) != *(rhs.args_[i])) {
            return false;
        }
    }

    return true;
}


bool FunctionCallExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const FunctionCallExpression&>(rhs);
    return name_ == r.name_ && *args_ == *(r.args_);
}



void FunctionCallExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // name_
    encoder << name_;

    // args_
    size_t sz = 0;
    if (args_) {
        sz = args_->numArgs();
    }
    encoder << sz;
    if (sz > 0) {
        for (const auto& arg : args_->args()) {
            encoder << *arg;
        }
    }
}


void FunctionCallExpression::resetFrom(Decoder& decoder) {
    // Read name_
    name_ = decoder.readStr();

    // Read args_
    size_t sz = decoder.readSize();
    args_ = ArgumentList::make(pool_);
    for (size_t i = 0;  i < sz; i++) {
        args_->addArgument(decoder.readExpression(pool_));
    }

    auto funcResult = FunctionManager::get(name_, args_->numArgs());
    if (funcResult.ok()) {
        func_ = std::move(funcResult).value();
    }
}

const Value& FunctionCallExpression::eval(ExpressionContext& ctx) {
    std::vector<std::reference_wrapper<const Value>> parameter;
    for (const auto& arg : DCHECK_NOTNULL(args_)->args()) {
        parameter.emplace_back(arg->eval(ctx));
    }
    result_ = DCHECK_NOTNULL(func_)(parameter);
    return result_;
}

std::string FunctionCallExpression::toString() const {
    std::vector<std::string> args(args_->numArgs());
    std::transform(args_->args().begin(),
                   args_->args().end(),
                   args.begin(),
                   [](const auto& arg) -> std::string { return arg->toString(); });
    std::stringstream out;
    out << name_ << "(" << folly::join(",", args) << ")";
    return out.str();
}

void FunctionCallExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}  // namespace nebula
