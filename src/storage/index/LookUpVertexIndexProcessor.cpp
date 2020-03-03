/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "LookUpVertexIndexProcessor.h"

namespace nebula {
namespace storage {

void LookUpVertexIndexProcessor::process(const cpp2::LookUpIndexRequest& req) {
    /**
     * step 1 : prepare index meta and structure of return columns.
     */
    auto ret = prepareRequest(req);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        putResultCodes(ret, req.get_parts());
        return;
    }

    /**
     * step 2 : build execution plan
     */
    ret = buildExecutionPlan(req.get_filter());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        putResultCodes(ret, req.get_parts());
        return;
    }

    /**
     * step 3 : execute index scan.
     */
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partId) {
        auto code = executeExecutionPlan(partId);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            VLOG(1) << "Error! ret = " << static_cast<int32_t>(code)
                    << ", spaceId = " << spaceId_
                    << ", partId =  " << partId;
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                this->handleLeaderChanged(spaceId_, partId);
            } else {
                this->pushResultCode(this->to(code), partId);
            }
        }
    });

    /**
     * step 4 : collect result.
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
    this->resp_.set_rows(std::move(vertexRows_));
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula

