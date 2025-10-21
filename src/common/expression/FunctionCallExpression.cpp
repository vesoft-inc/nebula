/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/FunctionCallExpression.h"

#include "common/expression/ExprVisitor.h"
#include "common/expression/LabelAttributeExpression.h"

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
  for (size_t i = 0; i < sz; i++) {
    args_->addArgument(decoder.readExpression(pool_));
  }

  auto funcResult = FunctionManager::get(name_, args_->numArgs());
  if (funcResult.ok()) {
    func_ = std::move(funcResult).value();
  }
}

const Value& FunctionCallExpression::eval(ExpressionContext& ctx) {
  LOG(ERROR) << "FunctionCallExpression::eval() - Function: " << name_
             << ", Args count: " << (args_ ? args_->numArgs() : 0);

  std::vector<std::reference_wrapper<const Value>> parameter;
  size_t argIdx = 0;
  for (const auto& arg : DCHECK_NOTNULL(args_)->args()) {
    // Log argument details before evaluation
    LOG(ERROR) << "  Arg[" << argIdx << "] - Kind: " << static_cast<int>(arg->kind())
               << ", Kind name: " << arg->kind() << ", toString: " << arg->toString();

    // Special handling for LabelAttributeExpression
    if (arg->kind() == Expression::Kind::kLabelAttribute) {
      auto* labelAttrExpr = static_cast<const LabelAttributeExpression*>(arg);
      LOG(ERROR) << "    LabelAttributeExpression detected:";
      LOG(ERROR) << "      Left (label): " << labelAttrExpr->left()->toString();
      LOG(ERROR) << "      Right (attribute): " << labelAttrExpr->right()->toString();
    }

    // Evaluate the argument
    const Value& argValue = arg->eval(ctx);

    // Log argument value after evaluation
    LOG(ERROR) << "  Arg[" << argIdx << "] evaluated - Type: " << argValue.type()
               << ", Value: " << argValue.toString() << ", isVector: " << argValue.isVector();

    parameter.emplace_back(argValue);
    argIdx++;
  }

  result_ = DCHECK_NOTNULL(func_)(parameter);

  LOG(ERROR) << "FunctionCallExpression::eval() - Function: " << name_
             << ", Result type: " << result_.type() << ", Result: " << result_.toString();

  return result_;
}

std::string FunctionCallExpression::toString() const {
  std::stringstream out;

  if (args_ != nullptr) {
    std::vector<std::string> args(args_->numArgs());
    std::transform(args_->args().begin(),
                   args_->args().end(),
                   args.begin(),
                   [](const auto& arg) -> std::string { return arg ? arg->toString() : ""; });
    out << name_ << "(" << folly::join(",", args) << ")";

  } else {
    out << name_ << "()";
  }
  return out.str();
}

void FunctionCallExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

}  // namespace nebula
