/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/AttributeExpression.h"

#include "common/datatypes/Edge.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/ExprVisitor.h"
#include "common/time/TimeUtils.h"

namespace nebula {

const Value &AttributeExpression::eval(ExpressionContext &ctx) {
  auto &lvalue = left()->eval(ctx);
  auto &rvalue = right()->eval(ctx);
  DCHECK(rvalue.isStr());

  // TODO(dutor) Take care of the builtin properties, _src, _vid, _type, etc.
  switch (lvalue.type()) {
    case Value::Type::MAP:
      return lvalue.getMap().at(rvalue.getStr());
    case Value::Type::VERTEX: {
      /*
       * WARNING(Xuntao): Vertices shall not be evaluated as AttributeExpressions,
       * since there shall always be a tag specified in the format of:
       * var.tag.property. Due to design flaws, however, we have to keep
       * this case segment.
       */
      if (rvalue.getStr() == kVid) {
        result_ = lvalue.getVertex().vid;
        return result_;
      }
      /*
       * WARNING(Xuntao): the following code snippet is dedicated to address the legacy
       * problem of treating LabelTagProperty expressions as Attribute expressions.
       * This snippet is necessary to allow users to write var.tag.prop in
       * ListComprehensionExpression without making structural changes to the implementation.
       */
      if (left()->kind() != Expression::Kind::kAttribute) {
        return lvalue;
      } else if (left()->kind() == Expression::Kind::kAttribute &&
                 dynamic_cast<AttributeExpression *>(left())->right()->kind() ==
                     Expression::Kind::kConstant) {
        auto &tagName = dynamic_cast<AttributeExpression *>(left())->right()->eval(ctx).getStr();
        for (auto &tag : lvalue.getVertex().tags) {
          if (tagName.compare(tag.name) != 0) {
            continue;
          } else {
            auto iter = tag.props.find(rvalue.getStr());
            if (iter != tag.props.end()) {
              return iter->second;
            }
          }
        }
      }
      return Value::kNullBadType;
    }
    case Value::Type::EDGE: {
      DCHECK(!rvalue.getStr().empty());
      if (rvalue.getStr()[0] == '_') {
        if (rvalue.getStr() == kSrc) {
          result_ = lvalue.getEdge().src;
        } else if (rvalue.getStr() == kDst) {
          result_ = lvalue.getEdge().dst;
        } else if (rvalue.getStr() == kRank) {
          result_ = lvalue.getEdge().ranking;
        } else if (rvalue.getStr() == kType) {
          result_ = lvalue.getEdge().name;
        }
        return result_;
      }
      auto iter = lvalue.getEdge().props.find(rvalue.getStr());
      if (iter == lvalue.getEdge().props.end()) {
        return Value::kNullValue;
      }
      return iter->second;
    }
    case Value::Type::DATE:
      result_ = time::TimeUtils::getDateAttr(lvalue.getDate(), rvalue.getStr());
      return result_;
    case Value::Type::TIME:
      result_ = time::TimeUtils::getTimeAttr(lvalue.getTime(), rvalue.getStr());
      return result_;
    case Value::Type::DATETIME:
      result_ = time::TimeUtils::getDateTimeAttr(lvalue.getDateTime(), rvalue.getStr());
      return result_;
    default:
      return Value::kNullBadType;
  }
}

void AttributeExpression::accept(ExprVisitor *visitor) {
  visitor->visit(this);
}

std::string AttributeExpression::toString() const {
  auto lhs = left();
  auto rhs = right();
  std::string buf;
  buf.reserve(256);
  buf += lhs ? lhs->toString() : "";
  buf += '.';
  if (rhs) {
    DCHECK_EQ(rhs->kind(), Kind::kConstant);
    auto *constant = static_cast<const ConstantExpression *>(rhs);
    if (constant->value().isStr()) {
      buf += constant->value().getStr();
    } else {
      buf += rhs->toString();
    }
  }

  return buf;
}

}  // namespace nebula
