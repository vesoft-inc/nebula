/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/SFIDExpression.h"

#include "common/id/SnowFlake.h"

namespace nebula {

bool SFIDExpression::operator==(const Expression& rhs) const { return kind_ == rhs.kind(); }

const Value& SFIDExpression::eval(ExpressionContext& ctx) {
  UNUSED(ctx);
  auto generator = new SnowFlake(workerId_);
  result_ = generator->getId();
  return result_;
}

std::string SFIDExpression::toString() const { return folly::stringPrintf("sfid()"); }

}  // namespace nebula
