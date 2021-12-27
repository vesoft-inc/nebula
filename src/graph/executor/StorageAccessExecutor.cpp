/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/StorageAccessExecutor.h"

#include <folly/Format.h>

#include "graph/context/Iterator.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"
#include "interface/gen-cpp2/meta_types.h"

using apache::thrift::optional_field_ref;

namespace nebula {
namespace graph {

namespace internal {

template <typename VidType>
struct Vid;

template <>
struct Vid<int64_t> {
  static int64_t value(const Value &v) {
    return v.getInt();
  }
};

template <>
struct Vid<std::string> {
  static std::string value(const Value &v) {
    return v.getStr();
  }
};

template <typename VidType>
DataSet buildRequestDataSet(const SpaceInfo &space,
                            QueryExpressionContext &exprCtx,
                            Iterator *iter,
                            Expression *expr,
                            bool dedup) {
  DCHECK(iter && expr) << "iter=" << iter << ", expr=" << expr;
  nebula::DataSet vertices({kVid});
  auto s = !iter->isGetNeighborsIter() ? iter->size() : 0;
  vertices.rows.reserve(s);

  std::unordered_set<VidType> uniqueSet;
  uniqueSet.reserve(s);

  const auto &vidType = *(space.spaceDesc.vid_type_ref());

  for (; iter->valid(); iter->next()) {
    auto vid = expr->eval(exprCtx(iter));
    if (!SchemaUtil::isValidVid(vid, vidType)) {
      LOG(WARNING) << "Mismatched vid type: " << vid.type()
                   << ", space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    if (dedup && !uniqueSet.emplace(Vid<VidType>::value(vid)).second) {
      continue;
    }
    vertices.emplace_back(Row({std::move(vid)}));
  }
  return vertices;
}

}  // namespace internal

bool StorageAccessExecutor::isIntVidType(const SpaceInfo &space) const {
  return (*space.spaceDesc.vid_type_ref()).type == nebula::cpp2::PropertyType::INT64;
}

DataSet StorageAccessExecutor::buildRequestDataSetByVidType(Iterator *iter,
                                                            Expression *expr,
                                                            bool dedup) {
  const auto &space = qctx()->rctx()->session()->space();
  QueryExpressionContext exprCtx(qctx()->ectx());

  if (isIntVidType(space)) {
    return internal::buildRequestDataSet<int64_t>(space, exprCtx, iter, expr, dedup);
  }
  return internal::buildRequestDataSet<std::string>(space, exprCtx, iter, expr, dedup);
}

std::string StorageAccessExecutor::getStorageDetail(
    optional_field_ref<const std::map<std::string, int32_t> &> ref) const {
  if (ref.has_value()) {
    auto content = util::join(*ref, [](auto &iter) -> std::string {
      return folly::sformat("{}:{}(us)", iter.first, iter.second);
    });
    return "{" + content + "}";
  }
  return "";
}

}  // namespace graph
}  // namespace nebula
