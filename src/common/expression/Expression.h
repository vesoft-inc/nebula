/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_EXPRESSION_H_
#define COMMON_EXPRESSION_EXPRESSION_H_

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"

namespace nebula {

class ExprVisitor;

class Expression {
 public:
  // Warning: this expression will be encoded and store in meta as default value
  // So don't add enum in middle, just append it!
  enum class Kind : uint8_t {
    kConstant,
    // Arithmetic
    kAdd,
    kMinus,
    kMultiply,
    kDivision,
    kMod,
    // Unary
    kUnaryPlus,
    kUnaryNegate,
    kUnaryNot,
    kUnaryIncr,
    kUnaryDecr,
    // Relational
    kRelEQ,
    kRelNE,
    kRelLT,
    kRelLE,
    kRelGT,
    kRelGE,
    kRelREG,
    kRelIn,
    kRelNotIn,
    kContains,
    kNotContains,
    kStartsWith,
    kNotStartsWith,
    kEndsWith,
    kNotEndsWith,

    kSubscript,
    kAttribute,
    kLabelAttribute,
    kColumn,
    // Logical
    kLogicalAnd,
    kLogicalOr,
    kLogicalXor,

    kTypeCasting,

    kFunctionCall,
    // Vertex/Edge/Path
    kTagProperty,
    kLabelTagProperty,
    kEdgeProperty,
    kInputProperty,
    kVarProperty,
    kDstProperty,
    kSrcProperty,
    kEdgeSrc,
    kEdgeType,
    kEdgeRank,
    kEdgeDst,
    kVertex,
    kEdge,

    kUUID,

    kVar,
    kVersionedVar,
    // Container
    kList,
    kSet,
    kMap,

    kLabel,

    kCase,

    kPredicate,
    kListComprehension,
    kReduce,

    kPathBuild,
    // text or key word search expression
    kTSPrefix,
    kTSWildcard,
    kTSRegexp,
    kTSFuzzy,

    kAggregate,
    kIsNull,
    kIsNotNull,
    kIsEmpty,
    kIsNotEmpty,

    kSubscriptRange,

    kMatchPathPattern,
  };

  Expression(ObjectPool* pool, Kind kind);
  virtual ~Expression() = default;

  Expression& operator=(const Expression& rhs) = delete;
  Expression& operator=(Expression&&) = delete;
  Expression(const Expression&) = delete;

  Kind kind() const {
    return kind_;
  }

  static Value eval(Expression* expr, ExpressionContext& ctx) {
    return expr->eval(ctx);
  }

  virtual const Value& eval(ExpressionContext& ctx) = 0;

  virtual bool operator==(const Expression& rhs) const = 0;
  bool operator!=(const Expression& rhs) const {
    return !operator==(rhs);
  }

  virtual std::string toString() const = 0;

  virtual std::string rawString() const {
    return toString();
  }

  virtual void accept(ExprVisitor* visitor) = 0;

  // Deep copy
  virtual Expression* clone() const = 0;

  std::string encode() const;

  static std::string encode(const Expression& exp);

  static Expression* decode(ObjectPool* pool, folly::StringPiece encoded);

  ObjectPool* getObjPool() const {
    return pool_;
  }

  virtual bool isLogicalExpr() const {
    return false;
  }

  virtual bool isRelExpr() const {
    return false;
  }

  virtual bool isArithmeticExpr() const {
    return false;
  }

  virtual bool isContainerExpr() const {
    return false;
  }

 protected:
  class Encoder final {
   public:
    explicit Encoder(size_t bufSizeHint = 2048);
    std::string moveStr();

    Encoder& operator<<(Kind kind);
    Encoder& operator<<(const std::string& str);
    Encoder& operator<<(const Value& val);
    Encoder& operator<<(size_t size);
    Encoder& operator<<(Value::Type vType);
    Encoder& operator<<(const Expression& exp);

   private:
    std::string buf_;
  };

  class Decoder final {
   public:
    explicit Decoder(folly::StringPiece encoded);

    bool finished() const;

    Kind readKind();
    std::string readStr();
    Value readValue();
    size_t readSize() noexcept;
    Value::Type readValueType() noexcept;
    Expression* readExpression(ObjectPool* pool);

    // Convert the unprocessed part into the hex string
    std::string getHexStr() const;

   private:
    folly::StringPiece encoded_;
    const char* ptr_;
  };

 protected:
  static Expression* decode(ObjectPool* pool, Decoder& decoder);

  // Serialize the content of the expression to the given encoder
  virtual void writeTo(Encoder& encoder) const = 0;

  // Reset the content of the expression from the given decoder
  virtual void resetFrom(Decoder& decoder) = 0;

  ObjectPool* pool_{nullptr};

  Kind kind_;
};

std::ostream& operator<<(std::ostream& os, Expression::Kind kind);

}  // namespace nebula

#endif  // COMMON_EXPRESSION_EXPRESSION_H_
