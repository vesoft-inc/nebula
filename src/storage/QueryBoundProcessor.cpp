/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/QueryBoundProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"


namespace nebula {
namespace storage {

void QueryBoundProcessor::prepareSchemata(
                                  const cpp2::GetNeighborsRequest& req,
                                  SchemaProviderIf*& edgeSchema,
                                  std::vector<std::pair<TagID, SchemaProviderIf*>>& tagSchemata,
                                  cpp2::Schema& respTag,
                                  cpp2::Schema& respEdge,
                                  int32_t& schemaVer) {

    VLOG(3) << "req.__isset " << req.__isset.edge_type << "," << req.__isset.ids;
    auto& props = req.get_return_columns();
    schemaVer = schemaMan_->version();
    edgeSchema = nullptr;
    if (req.__isset.edge_type) {
        edgeSchema = schemaMan_->edgeSchema(spaceId_, req.edge_type);
        respEdge.columns.reserve(props.size());
    }
    respTag.columns.reserve(props.size());
    std::unordered_map<TagID, SchemaProviderIf*> tagSchemaMap;
    for (auto& prop : props) {
        switch (prop.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                VLOG(3) << "tagId " << prop.tag_id << ", name " << prop.name;
                auto it = tagSchemaMap.find(prop.tag_id);
                SchemaProviderIf* schema = nullptr;
                if (it == tagSchemaMap.end()) {
                    schema = schemaMan_->tagSchema(spaceId_, prop.tag_id);
                    tagSchemaMap.emplace(prop.tag_id, schema);
                    tagSchemata.emplace_back(prop.tag_id, schema);
                } else {
                    schema = it->second;
                }
                CHECK_NOTNULL(schema);
                const auto* ftype = schema->getFieldType(prop.name, schemaVer);
                if (ftype != nullptr) {
                    respTag.columns.emplace_back(
                            columnDef(std::move(prop.get_name()), ftype->type));
                }
                break;
            }
            case cpp2::PropOwner::EDGE: {
                if (edgeSchema != nullptr) {
                    const auto* ftype = edgeSchema->getFieldType(prop.name, schemaVer);
                    if (ftype != nullptr) {
                        respEdge.columns.emplace_back(
                                columnDef(std::move(prop.get_name()), ftype->type));
                    }
                }
                break;
            }
        }
    };
}

kvstore::ResultCode QueryBoundProcessor::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            std::vector<std::pair<TagID, SchemaProviderIf*>> tagSchemata,
                            SchemaProviderIf* respTagSchema,
                            cpp2::VertexResponse& vResp) {
    RowWriter writer;
    kvstore::ResultCode ret;
    VLOG(1) << "collectVertexProps, tag schema number " << tagSchemata.size();
    for (auto& tagSchema : tagSchemata) {
        auto prefix = KeyUtils::prefix(partId, vId, tagSchema.first);
        std::unique_ptr<kvstore::StorageIter> iter;
        ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
        if (ret != kvstore::ResultCode::SUCCESSED) {
            break;
        }
        // Only get the latest version.
        if (iter && iter->valid()) {
            auto val = iter->val();
            // TODO pass the filter.
            output(tagSchema.second, val, 0, respTagSchema, writer);
        }
    };
    vResp.vertex_data = std::move(writer.encode());
    return ret;
}


kvstore::ResultCode QueryBoundProcessor::collectEdgesProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               SchemaProviderIf* edgeSchema,
                                               SchemaProviderIf* respEdgeSchema,
                                               cpp2::VertexResponse& vResp) {
    VLOG(1) << "collectEdgesProps...";
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    VLOG(3) << edgeSchema->getNumFields(0);
    if (iter) {
        RowSetWriter rsWriter;
        while(iter->valid()) {
            RowWriter writer;
            auto key = iter->key();
            UNUSED(key);
            auto val = iter->val();
            VLOG(3) << "key size " << key.size() << ", val size " << val.size();
            // TODO pass filter
            output(edgeSchema, val, 0, respEdgeSchema, writer);
            rsWriter.addRow(writer);
            iter->next();
        }
        vResp.edge_data = std::move(rsWriter.data());
    }
    return ret;
}


void QueryBoundProcessor::process(const cpp2::GetNeighborsRequest& req) {
    time::Duration dur;
    spaceId_ = req.get_space_id();

    int32_t schemaVer = 0;
    SchemaProviderIf* edgeSchema = nullptr;
    cpp2::Schema respTag, respEdge;
    std::vector<std::pair<TagID, SchemaProviderIf*>> tagSchemata;
    prepareSchemata(req, edgeSchema, tagSchemata,
                    respTag, respEdge, schemaVer);

    std::unique_ptr<SchemaProviderIf> respTagSchema;
    if (tagSchemata.size() > 0) {
        CHECK_GT(respTag.columns.size(), 0);
        respTagSchema.reset(new ResultSchemaProvider(respTag));
    }

    std::unique_ptr<SchemaProviderIf> respEdgeSchema;
    if (edgeSchema != nullptr) {
        respEdgeSchema.reset(new ResultSchemaProvider(respEdge));
    }

//    const auto& filter = req.get_filter();
    std::for_each(req.get_ids().begin(), req.get_ids().end(), [&](auto& partV) {
        auto partId = partV.first;
        kvstore::ResultCode ret;
        std::for_each(partV.second.begin(), partV.second.end(), [&](auto& vId) {
            VLOG(3) << "Process part " << partId << ", vertex " << vId;
            cpp2::VertexResponse vResp;
            vResp.vertex_id = vId;
            if (respTagSchema != nullptr) {
                VLOG(1) << "Query some tag/vertex props...";
                ret = collectVertexProps(partId, vId, tagSchemata,
                                         respTagSchema.get(), vResp);
            }
            if (respEdgeSchema != nullptr) {
                VLOG(1) << "Query some edge props...";
                ret = collectEdgesProps(partId, vId, req.edge_type, edgeSchema,
                                        respEdgeSchema.get(), vResp);
            }
            resp_.vertices.emplace_back(std::move(vResp));
        });
        // TODO handle failures
        cpp2::ResultCode thriftRet;
        thriftRet.code = to(ret);
        thriftRet.part_id = partId;
        resp_.codes.emplace_back(std::move(thriftRet));
    });
    resp_.vertex_schema = std::move(respTag);
    resp_.edge_schema = std::move(respEdge);
    resp_.latency_in_ms = dur.elapsedInMSec();
    onFinished();
}

}  // namespace storage
}  // namespace nebula
