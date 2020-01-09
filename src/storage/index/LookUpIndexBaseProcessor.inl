/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <interface/gen-cpp2/storage_types.h>
#include <interface/gen-cpp2/common_types.h>

DECLARE_int32(max_row_returned_per_index_scan);
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {
template<typename REQ, typename RESP>
cpp2::ErrorCode LookUpIndexBaseProcessor<REQ, RESP>::prepareLookUp(const REQ& req, bool isVertex) {
    spaceId_ = req.get_space_id();
    auto indexId = req.get_index_id();
    StatusOr<nebula::cpp2::IndexItem> ret;
    if (isVertex) {
        ret = this->schemaMan_->getTagIndex(spaceId_, indexId);
    } else {
        ret = this->schemaMan_->getEdgeIndex(spaceId_, indexId);
    }
    if (!ret.ok()) {
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    index_ = ret.value();
    tagOrEdge_ = index_.tagOrEdge;
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode LookUpIndexBaseProcessor<REQ, RESP>::prepareExpr(const REQ& req) {
    const auto& filterStr = req.get_filter();
    if (!filterStr.empty()) {
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(filterStr);
        if (!expRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        exp_ = std::move(expRet).value();
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
