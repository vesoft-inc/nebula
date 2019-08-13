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
        nebula::cpp2::Schema edgeSchema;
        decltype(edgeSchema.columns) cols;
        cols.reserve(props.size());
        for (auto& p : props) {
            cols.emplace_back(columnDef(p.prop_.name, p.type_.type));
        }

        if (!cols.empty()) {
            edgeSchema.set_columns(cols);
            vdata.edge_data.emplace_back(apache::thrift::FragileConstructor::FRAGILE, edgeType,
                                         edgeSchema, std::move(rsWriter.data()));
        }
    }

    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processAllEdge(PartitionID partId, VertexID vId,
                                                        FilterContext& fcontext,
                                                        cpp2::VertexData& vdata) {
    auto prefix = NebulaKeyUtils::prefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
        return ret;
    }

    std::unordered_set<EdgeType> edgeTypes;
    for (; iter->valid(); iter->next()) {
        auto key = iter->key();
        if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            if (edgeType < 0) {
                continue;
            }
            std::vector<PropContext> props;

            auto ite = edgeTypes.find(edgeType);
            if (ite != edgeTypes.end()) {
                continue;
            }

            edgeTypes.emplace(edgeType);
            auto it = edgeContext_.find(edgeType);
            if (it != edgeContext_.end()) {
                props = std::move(it->second);
            } else {
                PropContext pc;
                cpp2::PropDef pd;
                pc.pikType_ = PropContext::PropInKeyType::DST;
                pc.type_.type = nebula::cpp2::SupportedType::INT;
                pd.name = "_dst";
                pd.owner = storage::cpp2::PropOwner::EDGE;
                pc.prop_ = std::move(pd);
                pc.returned_ = true;
                props.emplace_back(pc);
            }
            ret = processEdgeImpl(partId, vId, edgeType, props, fcontext, vdata);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
    }

    return ret;
}

kvstore::ResultCode QueryBoundProcessor::processEdge(PartitionID partId, VertexID vId,
                                                     FilterContext& fcontext,
                                                     cpp2::VertexData& vdata) {
    for (const auto& ec : edgeContext_) {
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
            nebula::cpp2::Schema respTag;
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    respTag.columns.emplace_back(columnDef(prop.prop_.name, prop.type_.type));
                }
            }
            if (!respTag.get_columns().empty() && writer.size() > 1) {
                td.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                tc.tagId_,
                                std::move(respTag),
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
    if (overAllEdge_) {
        ret = processAllEdge(partId, vId, fcontext, vResp);
    } else {
        ret = processEdge(partId, vId, fcontext, vResp);
    }

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
}

}  // namespace storage
}  // namespace nebula
