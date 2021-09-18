/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "common/context/ExpressionContext.h"
#include "common/expression/Expression.h"
#include "folly/container/F14Map.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexSelectionNode : public IndexNode {
 public:
  IndexSelectionNode(RuntimeContext* context,
                     std::unique_ptr<Expression> expr,
                     const std::vector<std::string>& inputCols);
  nebula::cpp2::ErrorCode execute(PartitionID partId) override;
  ErrorOr<Row> next(bool& hasNext) override;

 private:
  inline bool filter(const Row& row) {
    ctx_->setRow(row);
    auto& result = expr_->eval(*ctx_);
    return result.type() == Value::Type::BOOL ? result.getBool() : false;
  }
  Map<std::string, size_t> colPos_;
  std::unique_ptr<Expression> expr_;
  class ExprContext : public ExpressionContext {
   public:
    explicit ExprContext(const Map<std::string, size_t>& colPos) : colPos_(colPos) {}
    void setRow(const Row& row) { row_ = &row; }
    Value getEdgeProp(const std::string& edgeType, const std::string& prop) const override;
    Value getTagProp(const std::string& tag, const std::string& prop) const override;
    // override
    const Value& getVar(const std::string& var) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    const Value& getVersionedVar(const std::string& var, int64_t version) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    const Value& getVarProp(const std::string& var, const std::string& prop) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }

    Value getSrcProp(const std::string& tag, const std::string& prop) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    const Value& getDstProp(const std::string& tag, const std::string& prop) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    const Value& getInputProp(const std::string& prop) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    Value getVertex() const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    Value getEdge() const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    Value getColumn(int32_t index) const override {
      LOG(FATAL) << "Unexpect";
      return Value();
    }
    void setVar(const std::string& var, Value val) override { LOG(FATAL) << "Unexpect"; }

   private:
    const Map<std::string, size_t>& colPos_;
    const Row* row_;
  };
  std::unique_ptr<ExprContext> ctx_;
};
}  // namespace storage

}  // namespace nebula
