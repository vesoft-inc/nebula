/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_UPDATERESULTNODE_H_
#define STORAGE_EXEC_UPDATERESULTNODE_H_

#include "common/base/Base.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/UpdateNode.h"

namespace nebula {
namespace storage {

/**
 * @brief UpdateResNode is used to calcaute the expressions whose results are need to return to
 * graphd
 *
 * UpdateResNode is used to evaluate the expression in the yield clause
 *
 * @tparam T
 *
 * @see RelNode<T>
 */
template <typename T>
class UpdateResNode : public RelNode<T> {
 public:
  using RelNode<T>::doExecute;

  /**
   * @brief Construct a new Update Res Node object
   *
   * @param context  Runtime context.
   * @param updateNode UpdateNode may be UpdateTagNode or UpdateEdgeNode.
   * @param returnPropsExp Expressions in yield clause.
   * @param expCtx Expression context
   */
  UpdateResNode(RuntimeContext* context,
                RelNode<T>* updateNode,
                std::vector<Expression*> returnPropsExp,
                StorageExpressionContext* expCtx,
                nebula::DataSet* result)
      : context_(context),
        updateNode_(updateNode),
        returnPropsExp_(returnPropsExp),
        expCtx_(expCtx),
        result_(result) {
    RelNode<T>::name_ = "UpdateResNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const T& vId) override {
    auto ret = RelNode<T>::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED && ret != nebula::cpp2::ErrorCode::E_FILTER_OUT) {
      return ret;
    }

    insert_ = context_->insert_;

    // Note: If filtered out, the result of tag prop is old
    result_->colNames.emplace_back("_inserted");
    std::vector<Value> row;
    row.emplace_back(insert_);

    for (auto& exp : returnPropsExp_) {
      auto& val = exp->eval(*expCtx_);
      if (exp) {
        result_->colNames.emplace_back(exp->toString());
      } else {
        VLOG(1) << "Can't get expression name";
        result_->colNames.emplace_back("NULL");
      }
      row.emplace_back(std::move(val));
    }
    result_->rows.emplace_back(std::move(row));
    return ret;
  }

 private:
  RuntimeContext* context_;
  RelNode<T>* updateNode_;
  std::vector<Expression*> returnPropsExp_;
  StorageExpressionContext* expCtx_;

  /**
   * @brief return prop sets
   *
   */
  nebula::DataSet* result_;
  bool insert_{false};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_UPDATERESULTNODE_H_
