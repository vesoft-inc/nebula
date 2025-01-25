/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/VectorQueryExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

bool VectorQueryArgument::operator==(const VectorQueryArgument& rhs) const {
  return index_ == rhs.index_ && field_ == rhs.field_;
}

std::string VectorQueryArgument::toString() const {
  std::string buf;
  buf.reserve(64);
  buf += index_ + ", ";
  buf += "\"" + field_ + "\"";
  return buf;
}

bool VectorQueryExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }
  const auto& r = dynamic_cast<const VectorQueryExpression&>(rhs);
  return arg_ == r.arg_;
}

std::string VectorQueryExpression::toString() const {
  std::string buf;
  buf.reserve(64);
  switch (kind_) {
    case Kind::kVectorQuery: {
      buf = "VECTOR_QUERY(";
      break;
    }
    default: {
      DLOG(FATAL) << "Illegal kind for vector search expression: " << static_cast<int>(kind());
      buf = "invalid vector search expression(";
    }
  }
  buf += arg_ ? arg_->toString() : "";
  buf += ")";
  return buf;
}

}  // namespace nebula 