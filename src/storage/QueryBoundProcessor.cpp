/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/QueryBoundProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode QueryBoundProcessor::checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) {
    if (this->expCtx_ == nullptr) {
        this->expCtx_ = std::make_unique<ExpressionContext>();
    }

    // return columns
    for (auto& col : req.get_return_columns()) {
        StatusOr<std::unique_ptr<Expression>> colExpRet = Expression::decode(col);
        if (!colExpRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_RETURN_COL;
        }
        auto colExp = std::move(colExpRet).value();
        colExp->setContext(this->expCtx_.get());
        auto status = colExp->prepare();
        if (!status.ok() || !this->checkExp(colExp.get())) {
            return cpp2::ErrorCode::E_INVALID_RETURN_COL;
        }
        returnColumnsExp_.emplace_back(std::move(colExp));
    }

    // filter(where/when)
    const auto& filterStr = req.get_filter();
    if (!filterStr.empty()) {
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(filterStr);
        if (!expRet.ok()) {
            VLOG(1) << "Can't decode the filter " << filterStr;
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        this->exp_ = std::move(expRet).value();
        this->exp_->setContext(this->expCtx_.get());
        auto status = this->exp_->prepare();
        if (!status.ok() || !this->checkExp(this->exp_.get())) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }

    // qwer: check expCtx_ has invalid property?

    return cpp2::ErrorCode::SUCCEEDED;
}

kvstore::ResultCode QueryBoundProcessor::processEdgeImpl(const PartitionID partId,
                                                         const VertexID vId,
                                                         const EdgeType edgeType,
                                                         const std::vector<PropContext>& props,
                                                         FilterContext& fcontext,
                                                         cpp2::VertexData& vdata) {
    RowSetWriter rsWriter;
    auto ret = collectEdgeProps(
        partId, vId, edgeType, props, &fcontext,
        [&, this](RowReader* reader, folly::StringPiece k, const std::vector<PropContext>& p) {
            RowWriter writer(rsWriter.schema());
            PropsCollector collector(&writer);
            this->collectProps(reader, k, p, &fcontext, &collector);
            rsWriter.addRow(writer);
        });
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (!rsWriter.data().empty()) {
        vdata.edge_data.emplace_back(apache::thrift::FragileConstructor::FRAGILE, edgeType,
                                     std::move(rsWriter.data()));
    }

    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processEdge(PartitionID partId, VertexID vId,
                                                     FilterContext& fcontext,
                                                     cpp2::VertexData& vdata) {
    for (const auto& ec : edgeContexts_) {
        RowSetWriter rsWriter;
        auto edgeType = ec.first;
        auto& props   = ec.second;
        if (!props.empty()) {
            CHECK(!onlyVertexProps_);

            auto ret = processEdgeImpl(partId, vId, edgeType, props, fcontext, vdata);

            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode QueryBoundProcessor::processVertex(PartitionID partId, VertexID vId) {
    cpp2::VertexData vResp;
    vResp.set_vertex_id(vId);
    FilterContext fcontext;
    if (!tagContexts_.empty()) {
        std::vector<cpp2::TagData> td;
        for (auto& tc : tagContexts_) {
            RowWriter writer;
            PropsCollector collector(&writer);
            VLOG(3) << "partId " << partId << ", vId " << vId << ", tagId " << tc.tagId_
                    << ", prop size " << tc.props_.size();
            auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_, &fcontext, &collector);
            if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                continue;
            }
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
            if (writer.size() > 1) {
                td.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                tc.tagId_,
                                writer.encode());
            }
        }
        vResp.set_tag_data(std::move(td));
    }

    if (onlyVertexProps_) {
        std::lock_guard<std::mutex> lg(this->lock_);
        vertices_.emplace_back(std::move(vResp));
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode ret;
    ret = processEdge(partId, vId, fcontext, vResp);

    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (!vResp.edge_data.empty()) {
        // Only return the vertex if edges existed.
        std::lock_guard<std::mutex> lg(this->lock_);
        vertices_.emplace_back(std::move(vResp));
    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundProcessor::onProcessFinished(int32_t retNum) {
    (void)retNum;
    resp_.set_vertices(std::move(vertices_));
    std::unordered_map<TagID, nebula::cpp2::Schema> vertexSchema;
    if (!this->tagContexts_.empty()) {
        for (auto& tc : this->tagContexts_) {
            nebula::cpp2::Schema respTag;
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    respTag.columns.emplace_back(
                        columnDef(std::move(prop.prop_.name), prop.type_.type));
                }
            }

            if (!respTag.columns.empty()) {
                auto it = vertexSchema.find(tc.tagId_);
                if (it == vertexSchema.end()) {
                    vertexSchema.emplace(tc.tagId_, respTag);
                }
            }
        }
        if (!vertexSchema.empty()) {
            resp_.set_vertex_schema(std::move(vertexSchema));
        }
    }

    std::unordered_map<EdgeType, nebula::cpp2::Schema> edgeSchema;
    if (!edgeContexts_.empty()) {
        for (const auto& ec : edgeContexts_) {
            nebula::cpp2::Schema respEdge;
            RowSetWriter rsWriter;
            auto& props = ec.second;
            for (auto& p : props) {
                respEdge.columns.emplace_back(columnDef(std::move(p.prop_.name), p.type_.type));
            }

            if (!respEdge.columns.empty()) {
                auto it = edgeSchema.find(ec.first);
                if (it == edgeSchema.end()) {
                    edgeSchema.emplace(ec.first, std::move(respEdge));
                }
            }
        }

        if (!edgeSchema.empty()) {
            resp_.set_edge_schema(std::move(edgeSchema));
        }
    }
}

void QueryBoundProcessor::process(const cpp2::GetNeighborsRequest& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();
    /*
    int32_t returnColumnsNum = req.get_return_columns().size();
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", return cols " << returnColumnsNum;
    // qwer
    // tagContexts_.reserve(returnColumnsNum);

    if (req.__isset.edge_types) {
        initEdgeContext(req.edge_types);
    }

    auto retCode = checkAndBuildContexts(req);

    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (const auto& part : req.get_parts()) {
            this->pushResultCode(retCode, part.first);
        }
        this->onFinished();
        return;
    }

    auto buckets = genBuckets(req);
    std::vector<folly::Future<std::vector<OneVertexResp>>> results;
    for (auto& bucket : buckets) {
        results.emplace_back(asyncProcessBucket(std::move(bucket)));
    }
    folly::collectAll(results).via(executor_).thenTry([
                     this,
                     returnColumnsNum] (auto&& t) mutable {
        CHECK(!t.hasException());
        std::unordered_set<PartitionID> failedParts;
        for (auto& bucketTry : t.value()) {
            CHECK(!bucketTry.hasException());
            for (auto& r : bucketTry.value()) {
                auto& partId = std::get<0>(r);
                auto& ret = std::get<2>(r);
                if (ret != kvstore::ResultCode::SUCCEEDED
                      && failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    this->pushResultCode(this->to(ret), partId);
                }
            }
        }
        this->onProcessFinished(returnColumnsNum);
        this->onFinished();
    });
    */
}

}  // namespace storage
}  // namespace nebula
