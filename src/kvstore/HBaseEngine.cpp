/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/HBaseEngine.h"
#include "storage/KeyUtils.h"
#include "dataman/NebulaCodecImpl.h"
#include "network/NetworkUtils.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace kvstore {

using nebula::storage::KeyUtils;

const char* kHBaseTableNamePrefix = "Nebula_Graph_Space_";
const char* kSchemaVersionColumnName = "__sv__";
const PartitionID kFillPartId = 0;
const uint8_t kFillMin = 0x00;
const uint8_t kFillMax = 0xFF;
const size_t kMaxRowKeyLength = sizeof(PartitionID) + sizeof(VertexID)
                              + sizeof(EdgeType) + sizeof(VertexID)
                              + sizeof(EdgeRanking) + sizeof(EdgeVersion);

HBaseEngine::HBaseEngine(GraphSpaceID spaceId,
                         const HostAddr& hbaseServer,
                         meta::SchemaManager* schemaMan)
        : KVEngine(spaceId)
        , schemaMan_(schemaMan) {
    CHECK_NOTNULL(schemaMan_);
    LOG(INFO) << "Connect to the HBase thrift server "
              << network::NetworkUtils::intToIPv4(hbaseServer.first)
              << ":" << hbaseServer.second;
    client_ = std::make_unique<HBaseClient>(hbaseServer);
    CHECK_NOTNULL(client_.get());
    tableName_ = kHBaseTableNamePrefix + folly::to<std::string>(spaceId_);
    partsNum_ = this->allParts().size();
    // TODO(zhangguoqing) check/create the hbase table named tableName_.
    // Now, the version 1.4.9 of HBase thrift2 does not support APIs for table operations.
    // client_->createTable(tableName_);
}


HBaseEngine::~HBaseEngine() {
}


std::shared_ptr<const meta::SchemaProviderIf> HBaseEngine::getSchema(const std::string& key,
                                                                     SchemaVer version) {
    std::shared_ptr<const meta::SchemaProviderIf> schema;
    folly::StringPiece rawKey = key;
    if (KeyUtils::isVertex(key)) {
        auto tagId = KeyUtils::getTagId(rawKey);
        if (version == -1) {
            version = this->schemaMan_->getNewestTagSchemaVer(spaceId_, tagId);
        }
        schema = this->schemaMan_->getTagSchema(spaceId_, tagId, version);
    } else if (KeyUtils::isEdge(key)) {
        auto edgeTypeId = KeyUtils::getEdgeType(rawKey);
        if (version == -1) {
            version = this->schemaMan_->getNewestEdgeSchemaVer(spaceId_, edgeTypeId);
        }
        schema = this->schemaMan_->getEdgeSchema(spaceId_, edgeTypeId, version);
    } else {
        LOG(ERROR) << "Key Type Error : " << key;
    }
    return std::move(schema);
}


std::string HBaseEngine::encode(const std::string& key,
                                KVMap& data) {
    std::string version = data[kSchemaVersionColumnName];
    auto schema = this->getSchema(key, folly::to<SchemaVer>(version));
    std::vector<dataman::Value> values;
    for (size_t index = 0; index < schema->getNumFields(); index++) {
        auto fieldName = schema->getFieldName(index);
        auto value = data[fieldName];
        switch (schema->getFieldType(index).get_type()) {
            case cpp2::SupportedType::INT:
                values.emplace_back(folly::to<int32_t>(value));
                break;
            case cpp2::SupportedType::STRING:
                values.emplace_back(folly::to<std::string>(value));
                break;
            case cpp2::SupportedType::VID:
                values.emplace_back(folly::to<int64_t>(value));
                break;
            case cpp2::SupportedType::FLOAT:
                values.emplace_back(folly::to<float>(value));
                break;
            case cpp2::SupportedType::DOUBLE:
                values.emplace_back(folly::to<double>(value));
                break;
            case cpp2::SupportedType::BOOL:
                values.emplace_back(folly::to<bool>(value));
                break;
            default:
                LOG(ERROR) << "Type Error : " << value;
                break;
        }
    }
    dataman::NebulaCodecImpl codec;
    return codec.encode(values, schema);
}


std::vector<KV> HBaseEngine::decode(const std::string& key,
                                    std::string& encoded) {
    auto schema = this->getSchema(key);
    dataman::NebulaCodecImpl codec;
    auto result = codec.decode(encoded, schema).value();
    std::vector<KV> data;
    auto version = schema->getVersion();
    data.emplace_back(kSchemaVersionColumnName, folly::to<std::string>(version));
    for (size_t index = 0; index < schema->getNumFields(); index++) {
        auto fieldName = schema->getFieldName(index);
        dataman::Value anyValue = result[fieldName];
        std::string value;
        if (anyValue.type() == typeid(int32_t)) {
            value = folly::to<std::string>(boost::any_cast<int32_t>(anyValue));
        } else if (anyValue.type() == typeid(std::string)) {
            value = folly::to<std::string>(boost::any_cast<std::string>(anyValue));
        } else if (anyValue.type() == typeid(int64_t)) {
            value = folly::to<std::string>(boost::any_cast<int64_t>(anyValue));
        } else if (anyValue.type() == typeid(float)) {
            value = folly::to<std::string>(boost::any_cast<float>(anyValue));
        } else if (anyValue.type() == typeid(double)) {
            value = folly::to<std::string>(boost::any_cast<double>(anyValue));
        } else if (anyValue.type() == typeid(bool)) {
            value = folly::to<std::string>(boost::any_cast<bool>(anyValue));
        } else {
            LOG(ERROR) << "Value Type :" << anyValue.type().name() << std::endl;
        }
        data.emplace_back(fieldName, value);
    }
    return data;
}


ResultCode HBaseEngine::get(const std::string& key, std::string* value) {
    // get data from HBase and encode it, then return
    auto rowKey = this->getRowKey(key);
    KVMap data;
    ResultCode code = client_->get(tableName_, rowKey, data);
    if (code == ResultCode::SUCCEEDED) {
        *value = this->encode(key, data);
    } else if (code == ResultCode::ERR_KEY_NOT_FOUND) {
        LOG(ERROR) << "Get: " << key << " Not Found";
    } else if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Get Failed: " << key << ", the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::multiGet(const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) {
    std::vector<std::string> rowKeys;
    for (auto& key : keys) {
        auto rowKey = this->getRowKey(key);
        rowKeys.emplace_back(rowKey);
    }
    std::vector<std::pair<std::string, KVMap>> dataList;
    ResultCode code = client_->multiGet(tableName_, rowKeys, dataList);
    for (size_t index = 0; index < dataList.size(); index++) {
        auto value = this->encode(keys[index], dataList[index].second);
        values->emplace_back(value);
    }
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "MultiGet Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::put(std::string key, std::string value) {
    // decode the value and store them into the HBase
    auto data = this->decode(key, value);
    auto rowKey = this->getRowKey(key);
    ResultCode code = client_->put(tableName_, rowKey, data);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Put Failed: " << key << ", the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::multiPut(std::vector<KV> keyValues) {
    std::vector<std::pair<std::string, std::vector<KV>>> dataList;
    for (size_t i = 0; i < keyValues.size(); i++) {
        auto rowKey = this->getRowKey(keyValues[i].first);
        auto data = this->decode(keyValues[i].first, keyValues[i].second);
        dataList.emplace_back(std::make_pair(rowKey, data));
    }
    ResultCode code = client_->multiPut(tableName_, dataList);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "MultiPut Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::range(const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* storageIter) {
    std::vector<std::pair<std::string, KVMap>> dataList;
    auto startRowKey = this->getRowKey(start);
    auto endRowKey = this->getRowKey(end);
    ResultCode code = client_->range(tableName_, startRowKey, endRowKey, dataList);
    std::vector<KV> keyValues;
    for (auto& data : dataList) {
        std::string rowKey = data.first;
        std::string key;
        key.reserve(sizeof(PartitionID) + rowKey.size());
        key.append(reinterpret_cast<const char*>(&kFillPartId), sizeof(PartitionID))
           .append(rowKey);
        auto value = this->encode(key, data.second);
        keyValues.emplace_back(std::make_pair(key, value));
    }
    storageIter->reset(new HBaseRangeIter(keyValues.begin(), keyValues.end()));
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Range Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::prefix(const std::string& prefix,
                               std::unique_ptr<KVIterator>* storageIter) {
    std::string startRowKey, endRowKey;
    startRowKey.reserve(kMaxRowKeyLength);
    endRowKey.reserve(kMaxRowKeyLength);
    startRowKey.append(prefix);
    endRowKey.append(prefix);
    for (size_t n = 0; n < kMaxRowKeyLength - prefix.size(); n++) {
        startRowKey.append(reinterpret_cast<const char*>(&kFillMin), sizeof(uint8_t));
        endRowKey.append(reinterpret_cast<const char*>(&kFillMax), sizeof(uint8_t));
    }
    ResultCode code = this->range(startRowKey, endRowKey, storageIter);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Prefix Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::remove(const std::string& key) {
    auto rowKey = this->getRowKey(key);
    ResultCode code = client_->remove(tableName_, rowKey);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Remove Failed: " << key << ", the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::multiRemove(std::vector<std::string> keys) {
    std::vector<std::string> rowKeys;
    for (size_t i = 0; i < keys.size(); i++) {
        auto rowKey = this->getRowKey(keys[i]);
        rowKeys.emplace_back(rowKey);
    }
    ResultCode code = client_->multiRemove(tableName_, rowKeys);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "MultiRemove Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseEngine::removeRange(const std::string& start,
                                    const std::string& end) {
    std::unique_ptr<kvstore::KVIterator> iter;
    ResultCode rangeCode = this->range(start, end, &iter);
    if (rangeCode == ResultCode::SUCCEEDED) {
        std::vector<std::string> keys;
        while (iter->valid()) {
            keys.emplace_back(iter->key());
            iter->next();
        }
        ResultCode code = this->multiRemove(keys);
        if (code == ResultCode::ERR_IO_ERROR) {
            LOG(ERROR) << "RemoveRange Failed: the HBase I/O error.";
        }
        return code;
    } else if (rangeCode == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "RemoveRange Failed on Range: the HBase I/O error.";
    }
    return rangeCode;
}


int32_t HBaseEngine::totalPartsNum() {
    return partsNum_;
}


void HBaseEngine::addPart(PartitionID partId) {
    UNUSED(partId);
    LOG(INFO) << "HBaseEngine does not support the \"addPart\" method.";
}


void HBaseEngine::removePart(PartitionID partId) {
    UNUSED(partId);
    LOG(INFO) << "HBaseEngine does not support the \"removePart\" method.";
}


std::vector<PartitionID> HBaseEngine::allParts() {
    std::vector<PartitionID> parts;
    parts.emplace_back(0);
    return parts;
}


ResultCode HBaseEngine::ingest(const std::vector<std::string>& files) {
    UNUSED(files);
    LOG(INFO) << "HBaseEngine does not support the \"ingest\" method.";
    return ResultCode::ERR_UNKNOWN;
}


ResultCode HBaseEngine::setOption(const std::string& config_key,
                                  const std::string& config_value) {
    UNUSED(config_key);
    UNUSED(config_value);
    LOG(INFO) << "HBaseEngine does not support the \"setOption\" method.";
    return ResultCode::ERR_UNKNOWN;
}


ResultCode HBaseEngine::setDBOption(const std::string& config_key,
                                    const std::string& config_value) {
    UNUSED(config_key);
    UNUSED(config_value);
    LOG(INFO) << "HBaseEngine does not support the \"setDBOption\" method.";
    return ResultCode::ERR_UNKNOWN;
}


ResultCode HBaseEngine::compactAll() {
    LOG(INFO) << "HBaseEngine does not support the \"compactAll\" method.";
    return ResultCode::ERR_UNKNOWN;
}

}  // namespace kvstore
}  // namespace nebula

