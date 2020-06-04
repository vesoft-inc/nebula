/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/FunctionCallExpression.h"

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

    const auto& r = dynamic_cast<const FunctionCallExpression&>(rhs);
    return *name_ == *(r.name_) && *args_ == *(r.args_);
}



void FunctionCallExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // name_
    encoder << name_.get();

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
    args_ = std::make_unique<ArgumentList>();
    for (size_t i = 0;  i < sz; i++) {
        args_->addArgument(decoder.readExpression());
    }
}


const Value& FunctionCallExpression::eval(ExpressionContext& ctx) {
    UNUSED(ctx);
    return result_;
}

}  // namespace nebula
