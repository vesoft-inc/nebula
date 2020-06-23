/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "tools/storage-client/src/lib/NebulaStorageClientBase.h"

namespace nebula {
IndexID NebulaStorageClientBase::getIndexId(bool isEdge, const std::string& name) {
    IndexID indexId = -1;
    auto index = isEdge
                 ? metaClient_->getEdgeIndexByNameFromCache(spaceId_, name)
                 : metaClient_->getTagIndexByNameFromCache(spaceId_, name);
    if (!index.ok()) {
        LOG(ERROR) << "Index not found : " << name;
        return indexId;
    }
    indexId = index.value()->get_index_id();
    return indexId;
}

int32_t NebulaStorageClientBase::getSchemaIdFromIndex(bool isEdge, IndexID indexId) {
    int32_t edgeOrTagId = -1;
    auto index = isEdge
                 ? metaClient_->getEdgeIndexFromCache(spaceId_, indexId)
                 : metaClient_->getTagIndexFromCache(spaceId_, indexId);
    if (!index.ok()) {
        LOG(ERROR) << "Index not found : " << indexId;
        return edgeOrTagId;
    }
    edgeOrTagId = isEdge
                  ? index.value()->get_schema_id().get_edge_type()
                  : index.value()->get_schema_id().get_tag_id();

    auto target = isEdge
                  ? metaClient_->getEdgeNameByTypeFromCache(spaceId_, edgeOrTagId)
                  : metaClient_->getTagNameByIdFromCache(spaceId_, edgeOrTagId);

    if (!target.ok()) {
        LOG(ERROR) << "Schema not found : " << edgeOrTagId;
        return -1;
    }
    targetSchemaName_ = std::move(target).value();
    return edgeOrTagId;
}

std::shared_ptr<const meta::SchemaProviderIf>
NebulaStorageClientBase::getLatestVersionSchema(bool isEdge, int32_t edgeOrTagId) {
    auto ver = isEdge
               ? metaClient_->getLatestEdgeVersionFromCache(spaceId_, edgeOrTagId)
               : metaClient_->getLatestTagVersionFromCache(spaceId_, edgeOrTagId);

    if (!ver.ok()) {
        LOG(ERROR) << "Schema not found : " << edgeOrTagId;
        return std::shared_ptr<const meta::SchemaProviderIf>();
    }

    auto edgeOrTag = isEdge
                     ? metaClient_->getEdgeSchemaFromCache(spaceId_, edgeOrTagId, ver.value())
                     : metaClient_->getTagSchemaFromCache(spaceId_, edgeOrTagId, ver.value());

    if (!edgeOrTag.ok()) {
        LOG(ERROR) << "Edge or Tag not found by id : " << edgeOrTagId;
        return std::shared_ptr<const meta::SchemaProviderIf>();
    }
    return edgeOrTag.value();
}

int32_t NebulaStorageClientBase::getSchemaIdByName(bool isEdge, const std::string& name) {
    int32_t schemaId = -1;
    auto ret = isEdge
               ? metaClient_->getEdgeTypeByNameFromCache(spaceId_, name)
               : metaClient_->getTagIDByNameFromCache(spaceId_, name);
    if (!ret.ok()) {
        return schemaId;
    }
    schemaId = ret.value();
    return schemaId;
}

std::shared_ptr<const meta::SchemaProviderIf>
NebulaStorageClientBase::getSchemaByName(bool isEdge, const std::string& name) {
    auto id = getSchemaIdByName(isEdge, name);
    if (id == -1) {
        return std::shared_ptr<const meta::SchemaProviderIf>();
    }
    return getLatestVersionSchema(isEdge, id);
}

DataType NebulaStorageClientBase::getColumnTypeBySchema(bool isEdge,
                                                        const std::string& schema,
                                                        const std::string& column) {
    auto sh = getSchemaByName(isEdge, schema);
    if (!sh) {
        LOG(ERROR) << "schema not found : " << schema;
    }
    auto t = toClientType(sh->getFieldType(column).type);
    return t;
}

DataType NebulaStorageClientBase::toClientType(cpp2::SupportedType type) {
    DataType t = DataType::UNKNOWN;
    switch (type) {
        case cpp2::SupportedType::VID : {
            t = DataType::VID;
            break;
        }
        case cpp2::SupportedType::BOOL : {
            t = DataType::BOOL;
            break;
        }
        case cpp2::SupportedType::INT : {
            t = DataType::INT;
            break;
        }
        case cpp2::SupportedType::TIMESTAMP : {
            t = DataType::TIMESTAMP;
            break;
        }
        case cpp2::SupportedType::FLOAT : {
            t = DataType::FLOAT;
            break;
        }
        case cpp2::SupportedType::DOUBLE : {
            t = DataType::DOUBLE;
            break;
        }
        case cpp2::SupportedType::STRING : {
            t = DataType::STRING;
            break;
        }
        default : {
            return t;
        }
    }
    return t;
}
void NebulaStorageClientBase::writeEdgeIndexResult(
    const nebula::storage::cpp2::Edge& edge,
    std::shared_ptr<const meta::SchemaProviderIf> schema,
    ResultSet& resultSet) {
    std::vector<Val> record;
    const auto& edgeKey = edge.get_key();
    record.emplace_back(edgeKey.get_src());
    record.emplace_back(edgeKey.get_dst());
    record.emplace_back(edgeKey.get_ranking());
    auto reader = RowReader::getRowReader(edge.get_props(), std::move(schema));
    for (size_t i = 3 ; i < resultSet.returnCols.size(); i++) {
        auto res = RowReader::getPropByName(reader.get(), resultSet.returnCols[i].column_);
        if (!ok(res)) {
            LOG(ERROR) << "Skip the bad value " << resultSet.returnCols[i].column_;
            return;
        }
        auto&& v = value(std::move(res));
        record.emplace_back(getValueFromVariant(v));
    }
    resultSet.rows.emplace_back(std::move(record));
}

void NebulaStorageClientBase::writeTagIndexResult(
    const nebula::storage::cpp2::VertexIndexData& vertexIndexData,
    std::shared_ptr<const meta::SchemaProviderIf> schema,
    ResultSet& resultSet) {
    std::vector<Val> record;
    record.emplace_back(vertexIndexData.get_vertex_id());
    auto reader = RowReader::getRowReader(vertexIndexData.get_props(), std::move(schema));
    for (size_t i = 1 ; i < resultSet.returnCols.size(); i++) {
        auto res = RowReader::getPropByName(reader.get(), resultSet.returnCols[i].column_);
        if (!ok(res)) {
            LOG(ERROR) << "Skip the bad value " << resultSet.returnCols[i].column_;
            return;
        }
        auto&& val = value(std::move(res));
        record.emplace_back(getValueFromVariant(val));
    }
    resultSet.rows.emplace_back(std::move(record));
}

void NebulaStorageClientBase::writeQueryResult(const storage::cpp2::QueryResponse& resp,
                                               ResultSet& resultSet) {
    auto row = initEmptyResultRow(resultSet.returnCols);
    const std::unordered_map<cpp2::TagID, cpp2::Schema>* vschema = nullptr;
    const std::unordered_map<cpp2::EdgeType, cpp2::Schema>* eschema = nullptr;
    if (resp.__isset.vertex_schema) {
        vschema = resp.get_vertex_schema();
    }
    if (resp.__isset.edge_schema) {
        eschema = resp.get_edge_schema();
    }

    std::unordered_map<int32_t, std::shared_ptr<ResultSchemaProvider>> schema;

    if (vschema) {
        for (const auto& v : *vschema) {
            schema[v.first] = std::make_shared<ResultSchemaProvider>(v.second);
        }
    }
    if (eschema) {
        for (const auto& e : *eschema) {
            schema[e.first] = std::make_shared<ResultSchemaProvider>(e.second);
        }
    }

    for (auto& vp : resp.vertices) {
        for (auto& ep : vp.edge_data) {
            auto edgeType = ep.type;
            for (const auto& edge : ep.get_edges()) {
                clearResultRow(row);
                auto dst = edge.get_dst();
                auto dIt = row.find(std::make_pair(edgeType, "_dst"));
                if (dIt != row.end()) {
                    dIt->second = Val(dst);
                }
                const auto& val = edge.get_props();
                writeQueryResultCell(val, edgeType, row, schema);
                // write tag props.
                const auto& tagData = vp.get_tag_data();
                for (const auto& tag : tagData) {
                    auto tagId = tag.get_tag_id();
                    const auto& v = tag.get_data();
                    writeQueryResultCell(v, tagId, row, schema);
                }
                insertRow(row, resultSet);
            }
        }
    }
}

void NebulaStorageClientBase::writeVertexPropResult(const storage::cpp2::QueryResponse& resp,
                                                    ResultSet& resultSet) {
    auto row = initEmptyResultRow(resultSet.returnCols);
    const std::unordered_map<cpp2::TagID, cpp2::Schema>* vschema = nullptr;
    if (resp.__isset.vertex_schema) {
        vschema = resp.get_vertex_schema();
    }

    std::unordered_map<int32_t, std::shared_ptr<ResultSchemaProvider>> schema;

    if (vschema) {
        for (const auto& v : *vschema) {
            schema[v.first] = std::make_shared<ResultSchemaProvider>(v.second);
        }
    }
    for (auto& vp : resp.vertices) {
        auto dIt = row.find(std::make_pair(0, "VertexID"));
        if (dIt != row.end()) {
            dIt->second = Val(vp.get_vertex_id());
        }
        // write tag props.
        const auto& tagData = vp.get_tag_data();
        for (const auto& tag : tagData) {
            auto tagId = tag.get_tag_id();
            const auto& v = tag.get_data();
            writeQueryResultCell(v, tagId, row, schema);
        }
        insertRow(row, resultSet);
    }
}

QueryResultRow NebulaStorageClientBase::initEmptyResultRow(const std::vector<ColumnDef>& cols) {
    QueryResultRow row;
    for (const auto& col : cols) {
        int32_t id = -1;
        if (col.edgeOrTag_.empty()) {
            id = 0;
        } else {
            id = (col.owner_ == PropOwner::EDGE) ? edges_[col.edgeOrTag_] : tags_[col.edgeOrTag_];
        }
        auto mapKey = std::make_pair(id, col.column_.c_str());
        row[mapKey] = Val();
    }
    return row;
}

void NebulaStorageClientBase::clearResultRow(QueryResultRow& row) {
    for (auto & iter : row) {
        iter.second = Val();
    }
}

void NebulaStorageClientBase::insertRow(const QueryResultRow& row, ResultSet& resultSet) {
    std::vector<Val> r;
    for (const auto& col : resultSet.returnCols) {
        auto id = (col.owner_ == PropOwner::EDGE) ? edges_[col.edgeOrTag_] : tags_[col.edgeOrTag_];
        auto mapKey = std::make_pair(id, col.column_.c_str());
        auto it = row.find(mapKey);
        if (it == row.end()) {
            continue;
        }
        r.emplace_back(it->second);
    }
    resultSet.rows.emplace_back(std::move(r));
}

void NebulaStorageClientBase::writeQueryResultCell(const std::string& val, int32_t id,
                                                   QueryResultRow& row,
                                                   const std::unordered_map<int32_t,
                                                   std::shared_ptr<ResultSchemaProvider>>& schema) {
    auto it = schema.find(id);
    if (it != schema.end()) {
        for (auto & iter : row) {
            if (iter.first.first == id) {
                auto reader = RowReader::getRowReader(val, it->second);
                if (reader != nullptr && iter.first.second != "_dst") {
                    auto res = RowReader::getPropByName(reader.get(), iter.first.second);
                    if (ok(res)) {
                        auto v = value(res);
                        iter.second = getValueFromVariant(v);
                    }
                }
            }
        }
    }
}

storage::cpp2::PropOwner NebulaStorageClientBase::toPropOwner(PropOwner owner) {
    switch (owner) {
        case PropOwner::TAG : {
            return storage::cpp2::PropOwner::SOURCE;
        }
        case PropOwner::EDGE : {
            return storage::cpp2::PropOwner::EDGE;
        }
    }
    return storage::cpp2::PropOwner::SOURCE;
}

Val NebulaStorageClientBase::getValueFromVariant(const VariantType& v) {
    switch (v.which()) {
        case 0 : {
            return Val(boost::get<int64_t>(v));
        }
        case 1 : {
            return Val(boost::get<double>(v));
        }
        case 2 : {
            return Val(boost::get<bool>(v));
        }
        case 3 : {
            return Val(boost::get<std::string>(v));
        }
    }
    return Val();
}

nebula::ResultCode NebulaStorageClientBase::toResultCode(nebula::storage::cpp2::ErrorCode code) {
    nebula::ResultCode ret = nebula::ResultCode::E_UNKNOWN;
    switch (code) {
        case nebula::storage::cpp2::ErrorCode::SUCCEEDED : {
            ret = nebula::ResultCode::SUCCEEDED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_DISCONNECTED : {
            ret = nebula::ResultCode::E_DISCONNECTED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_FAILED_TO_CONNECT : {
            ret = nebula::ResultCode::E_FAILED_TO_CONNECT;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_RPC_FAILURE : {
            ret = nebula::ResultCode::E_RPC_FAILURE;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_LEADER_CHANGED : {
            ret = nebula::ResultCode::E_LEADER_CHANGED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_KEY_HAS_EXISTS : {
            ret = nebula::ResultCode::E_KEY_HAS_EXISTS;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND : {
            ret = nebula::ResultCode::E_SPACE_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_PART_NOT_FOUND : {
            ret = nebula::ResultCode::E_PART_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_KEY_NOT_FOUND : {
            ret = nebula::ResultCode::E_KEY_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_CONSENSUS_ERROR : {
            ret = nebula::ResultCode::E_CONSENSUS_ERROR;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND : {
            ret = nebula::ResultCode::E_EDGE_PROP_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND : {
            ret = nebula::ResultCode::E_TAG_PROP_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_IMPROPER_DATA_TYPE : {
            ret = nebula::ResultCode::E_IMPROPER_DATA_TYPE;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_EDGE_NOT_FOUND : {
            ret = nebula::ResultCode::E_EDGE_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_TAG_NOT_FOUND : {
            ret = nebula::ResultCode::E_TAG_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_INDEX_NOT_FOUND : {
            ret = nebula::ResultCode::E_INDEX_NOT_FOUND;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER : {
            ret = nebula::ResultCode::E_INVALID_FILTER;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER : {
            ret = nebula::ResultCode::E_INVALID_UPDATER;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_INVALID_STORE : {
            ret = nebula::ResultCode::E_INVALID_STORE;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_INVALID_PEER : {
            ret = nebula::ResultCode::E_INVALID_PEER;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_RETRY_EXHAUSTED : {
            ret = nebula::ResultCode::E_RETRY_EXHAUSTED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_TRANSFER_LEADER_FAILED : {
            ret = nebula::ResultCode::E_TRANSFER_LEADER_FAILED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_LOAD_META_FAILED : {
            ret = nebula::ResultCode::E_LOAD_META_FAILED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT : {
            ret = nebula::ResultCode::E_FAILED_TO_CHECKPOINT;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_CHECKPOINT_BLOCKED : {
            ret = nebula::ResultCode::E_CHECKPOINT_BLOCKED;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_FILTER_OUT : {
            ret = nebula::ResultCode::E_FILTER_OUT;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_PARTIAL_RESULT : {
            ret = nebula::ResultCode::E_PARTIAL_RESULT;
            break;
        }
        case nebula::storage::cpp2::ErrorCode::E_UNKNOWN : {
            ret = nebula::ResultCode::E_UNKNOWN;
            break;
        }
    }
    return ret;
}
}   // namespace nebula
