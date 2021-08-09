/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/StorageAccessExecutor.h"

#include "common/interface/gen-cpp2/meta_types.h"
#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "util/SchemaUtil.h"

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
    vertices.rows.reserve(iter->size());

    std::unordered_set<VidType> uniqueSet;
    uniqueSet.reserve(iter->size());

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

}   // namespace internal

bool StorageAccessExecutor::isIntVidType(const SpaceInfo &space) const {
    return (*space.spaceDesc.vid_type_ref()).type == meta::cpp2::PropertyType::INT64;
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

}   // namespace graph
}   // namespace nebula
