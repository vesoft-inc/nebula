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

#define CHECK_AND_WRITE(ret) \
    if (ResultType::SUCCEEDED == ret) { \
        LOG(INFO) << name << "," << v; \
        writer << v; \
    }

namespace nebula {
namespace storage {

cpp2::ColumnDef QueryBoundProcessor::columnDef(std::string name, cpp2::SupportedType type) {
    cpp2::ColumnDef column;
    column.name = std::move(name);
    column.type.type = type;
    return column;
}

void
QueryBoundProcessor::prepareSchemata(const std::vector<cpp2::PropDef>& props,
                                     EdgeType edgeType,
                                     SchemaProviderIf*& edgeSchema,
                                     std::vector<std::pair<TagID, SchemaProviderIf*>>& tagSchemata,
                                     cpp2::Schema& respTag,
                                     cpp2::Schema& respEdge,
                                     int32_t& schemaVer) {
    edgeSchema = schemaMan_->edgeSchema(spaceId_, edgeType);
    schemaVer = schemaMan_->version();
    respEdge.columns.reserve(props.size());
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
                const auto* ftype = edgeSchema->getFieldType(prop.name, schemaVer);
                if (ftype != nullptr) {
                    respEdge.columns.emplace_back(
                            columnDef(std::move(prop.get_name()), ftype->type));
                }
                break;
            }
        }
    };
}

void QueryBoundProcessor::output(SchemaProviderIf* inputSchema,
                                 folly::StringPiece inputRow,
                                 int32_t ver,
                                 SchemaProviderIf* outputSchema,
                                 RowWriter& writer) {
    CHECK_NOTNULL(inputSchema);
    CHECK_NOTNULL(outputSchema);
    for (auto i = 0; i < inputSchema->getNumFields(ver); i++) {
        const auto* name = inputSchema->getFieldName(i, ver);
        VLOG(3) << "input schema: name = " << name;
    }
    RowReader reader(inputSchema, inputRow);
    auto numFields = outputSchema->getNumFields(ver);
    VLOG(3) << "Total output columns is " << numFields;
    for (auto i = 0; i < numFields; i++) {
        const auto* name = outputSchema->getFieldName(i, ver);
        const auto* type = outputSchema->getFieldType(i, ver);
        VLOG(3) << "output schema " << name;
        switch (type->type) {
            case cpp2::SupportedType::BOOL: {
                bool v;
                auto ret = reader.getBool(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            case cpp2::SupportedType::INT: {
                int32_t v;
                auto ret = reader.getInt<int32_t>(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            case cpp2::SupportedType::VID: {
                int64_t v;
                auto ret = reader.getVid(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                float v;
                auto ret = reader.getFloat(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                double v;
                auto ret = reader.getDouble(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            case cpp2::SupportedType::STRING: {
                folly::StringPiece v;
                auto ret = reader.getString(name, v);
                CHECK_AND_WRITE(ret);
                break;
            }
            default: {
                LOG(FATAL) << "Unsupport type!";
                break;
            }
        }
    };
}

kvstore::ResultCode
QueryBoundProcessor::collectVertexProps(PartitionID partId, VertexID vId,
                                        std::vector<std::pair<TagID, SchemaProviderIf*>> tagSchemata,
                                        SchemaProviderIf* respTagSchema,
                                        cpp2::VertexResponse& vResp) {
    if (tagSchemata.empty()) {
        return kvstore::ResultCode::SUCCESSED;
    }
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


kvstore::ResultCode
QueryBoundProcessor::collectEdgesProps(PartitionID partId,
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
    auto edgeType = req.get_edge_type();

    int32_t schemaVer = 0;
    SchemaProviderIf* edgeSchema;
    cpp2::Schema respTag, respEdge;
    std::vector<std::pair<TagID, SchemaProviderIf*>> tagSchemata;
    prepareSchemata(req.get_return_columns(), edgeType,
                    edgeSchema, tagSchemata,
                    respTag, respEdge, schemaVer);

    std::unique_ptr<SchemaProviderIf> respTagSchema(new ResultSchemaProvider(respTag));
    std::unique_ptr<SchemaProviderIf> respEdgeSchema(new ResultSchemaProvider(respEdge));

//    const auto& filter = req.get_filter();
    std::for_each(req.get_ids().begin(), req.get_ids().end(), [&](auto& partV) {
        auto partId = partV.first;
        kvstore::ResultCode ret;
        std::for_each(partV.second.begin(), partV.second.end(), [&](auto& vId) {
            VLOG(3) << "Process part " << partId << ", vertex " << vId;
            cpp2::VertexResponse vResp;
            vResp.vertex_id = vId;
            ret = collectVertexProps(partId, vId, tagSchemata,
                                     respTagSchema.get(), vResp);
            ret = collectEdgesProps(partId, vId, edgeType, edgeSchema,
                                    respEdgeSchema.get(), vResp);
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
