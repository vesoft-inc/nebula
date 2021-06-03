/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "LookUpIndexProcessor.h"

namespace nebula {
namespace storage {

void LookUpIndexProcessor::process(const cpp2::LookUpIndexRequest& req) {
    /**
     * step 1 : prepare index meta and structure of return columns.
     */
    auto ret = prepareRequest(req);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Prepare Request Failed";
        putResultCodes(ret, req.get_parts());
        return;
    }

    /**
     * step 2 : build execution plan
     */
    ret = buildExecutionPlan(req.get_filter());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Build Execution Plan Failed";
        putResultCodes(ret, req.get_parts());
        return;
    }

    /**
     * step 3 : setup scan pair by parts
     */
    if (!makeScanPairByParts(req.get_parts())) {
        LOG(ERROR) << "Build Execution Plan Failed";
        putResultCodes(cpp2::ErrorCode::E_INVALID_FILTER, req.get_parts());
        return;
    }

    /**
     * step 4 : execute index scan.
     */
    if (executor_ == nullptr) {
        for (auto partId : req.get_parts()) {
            auto code = executeExecutionPlan(partId);
            if (code != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Execute Execution Plan! ret = " << static_cast<int32_t>(code)
                           << ", spaceId = " << spaceId_
                           << ", partId =  " << partId;
                if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                    this->handleLeaderChanged(spaceId_, partId);
                } else {
                    this->pushResultCode(this->to(code), partId);
                }
                this->onFinished();
                return;
            }
        }
    } else {
        auto future = executeExecutionPlanConcurrently(req.get_parts());
        auto map = std::move(future).get();
        if (!map.empty()) {
            for (auto& errorPart : map) {
                if (errorPart.second == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                    this->handleLeaderChanged(spaceId_, errorPart.first);
                } else {
                    this->pushResultCode(this->to(errorPart.second), errorPart.first);
                }
            }
            this->onFinished();
            return;
        }
    }


    /**
     * step 5 : collect result.
     */
    if (schema_ != nullptr) {
        decltype(resp_.schema) s;
        decltype(resp_.schema.columns) cols;
        for (auto i = 0; i < static_cast<int64_t>(schema_->getNumFields()); i++) {
            cols.emplace_back(columnDef(schema_->getFieldName(i),
                                        schema_->getFieldType(i).get_type()));
        }
        s.set_columns(std::move(cols));
        this->resp_.set_schema(std::move(s));
    }

    if (isEdgeIndex_) {
        this->resp_.set_edges(std::move(edgeRows_));
    } else {
        this->resp_.set_vertices(std::move(vertexRows_));
    }

    this->onFinished();
}

}  // namespace storage
}  // namespace nebula

