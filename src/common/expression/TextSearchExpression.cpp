/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/TextSearchExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

bool TextSearchArgument::operator==(const TextSearchArgument& rhs) const {
  return index_ == rhs.index_ && query_ == rhs.query_ && props_ == rhs.props_;
}

std::string TextSearchArgument::toString() const {
  std::string buf;
  buf.reserve(64);
  if (!index_.empty()) {
    buf += "\"", index_ + "\", ";
  }
  buf += "\"" + query_ + "\"";
  if (!props_.empty()) {
    buf += ", [";
    for (size_t i = 0; i < props_.size(); i++) {
      buf += props_[i];
      if (i != props_.size() - 1) {
        buf += ", ";
      }
    }
    buf += "]";
  }

  if (count_ != 0) {
    buf += ", " + std::to_string(count_);
    if (offset_) {
      buf += ", " + std::to_string(offset_);
    }
  }

  return buf;
}

bool TextSearchExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }

  const auto& r = dynamic_cast<const TextSearchExpression&>(rhs);
  return arg_ == r.arg_;
}

std::string TextSearchExpression::toString() const {
  std::string buf;
  buf.reserve(64);
  switch (kind_) {
    case Kind::kESMATCH: {
      buf = "ES_MATCH(";
      break;
    }
    case Kind::kESQUERY: {
      buf = "ES_QUERY(";
      break;
    }
    default: {
      DLOG(FATAL) << "Illegal kind for text search expression: " << static_cast<int>(kind());
      buf = "invalid text search expression(";
    }
  }
  buf += arg_ ? arg_->toString() : "";
  buf += ")";
  return buf;
}
}  // namespace nebula
