/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXEXPRCONTEXT_H
#define STORAGE_EXEC_INDEXEXPRCONTEXT_H

#include "common/expression/Expression.h"
#include "storage/exec/IndexNode.h"

namespace nebula {
namespace storage {

class IndexExprContext : public ExpressionContext {
 public:
  explicit IndexExprContext(const Map<std::string, size_t> &colPos) : colPos_(colPos) {}
  void setRow(const Row &row) {
    row_ = &row;
  }

  Value getEdgeProp(const std::string &edgeType, const std::string &prop) const override {
    UNUSED(edgeType);
    DCHECK(row_ != nullptr);
    auto iter = colPos_.find(prop);
    DCHECK(iter != colPos_.end());
    DCHECK(iter->second < row_->size());
    return (*row_)[iter->second];
  }

  Value getTagProp(const std::string &tag, const std::string &prop) const override {
    UNUSED(tag);
    DCHECK(row_ != nullptr);
    auto iter = colPos_.find(prop);
    DCHECK(iter != colPos_.end());
    DCHECK(iter->second < row_->size());
    return (*row_)[iter->second];
  }

  // override
  const Value &getVar(const std::string &var) const override {
    UNUSED(var);
    return fatal(__FILE__, __LINE__);
  }
  const Value &getVersionedVar(const std::string &var, int64_t version) const override {
    UNUSED(var), UNUSED(version);
    return fatal(__FILE__, __LINE__);
  }
  const Value &getVarProp(const std::string &var, const std::string &prop) const override {
    UNUSED(var), UNUSED(prop);
    return fatal(__FILE__, __LINE__);
  }
  Value getSrcProp(const std::string &tag, const std::string &prop) const override {
    UNUSED(tag), UNUSED(prop);
    return fatal(__FILE__, __LINE__);
  }
  const Value &getDstProp(const std::string &tag, const std::string &prop) const override {
    UNUSED(tag), UNUSED(prop);
    return fatal(__FILE__, __LINE__);
  }
  const Value &getInputProp(const std::string &prop) const override {
    UNUSED(prop);
    return fatal(__FILE__, __LINE__);
  }
  Value getVertex(const std::string &) const override {
    return fatal(__FILE__, __LINE__);
  }
  Value getEdge() const override {
    return fatal(__FILE__, __LINE__);
  }
  Value getColumn(int32_t index) const override {
    UNUSED(index);
    return fatal(__FILE__, __LINE__);
  }
  void setVar(const std::string &var, Value val) override {
    UNUSED(var), UNUSED(val);
    fatal(__FILE__, __LINE__);
  }

 private:
  const Map<std::string, size_t> &colPos_;
  const Row *row_;
  inline const Value &fatal(const std::string &file, int line) const {
    LOG(FATAL) << "Unexpect at " << file << ":" << line;
    static Value placeholder;
    return placeholder;
  }
};

}  // namespace storage
}  // namespace nebula
#endif
