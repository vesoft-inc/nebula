/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include "network/NetworkUtils.h"
#include "dataman/NebulaCodecImpl.h"
#include "kvstore/plugins/hbase/HBaseStore.h"
#include <folly/Likely.h>

namespace nebula {
namespace kvstore {

const char* kHBaseTableNamePrefix = "Nebula_Graph_Space_";
const char* kSchemaVersionColumnName = "__sv__";
const PartitionID kFillPartId = 0;
const uint8_t kFillMin = 0x00;
const uint8_t kFillMax = 0xFF;
const size_t kMaxRowKeyLength = sizeof(PartitionID) + sizeof(VertexID)
                              + sizeof(EdgeType) + sizeof(VertexID)
                              + sizeof(EdgeRanking) + sizeof(EdgeVersion);

HBaseStore::HBaseStore(KVOptions options)
        : options_(std::move(options)) {
    schemaMan_ = std::move(options_.schemaMan_);
    CHECK_NOTNULL(schemaMan_);
}


void HBaseStore::init() {
    LOG(INFO) << "Connect to the HBase thrift server "
              << network::NetworkUtils::intToIPv4(options_.hbaseServer_.first)
              << ":" << options_.hbaseServer_.second;
    client_ = std::make_unique<HBaseClient>(options_.hbaseServer_);
    CHECK_NOTNULL(client_.get());
}


std::string HBaseStore::spaceIdToTableName(GraphSpaceID spaceId) {
    return kHBaseTableNamePrefix + folly::to<std::string>(spaceId);
}


std::shared_ptr<const meta::SchemaProviderIf> HBaseStore::getSchema(GraphSpaceID spaceId,
                                                                    const std::string& key,
                                                                    SchemaVer version) {
    std::shared_ptr<const meta::SchemaProviderIf> schema;
    folly::StringPiece rawKey = key;
    if (NebulaKeyUtils::isVertex(key)) {
        TagID tagId = NebulaKeyUtils::getTagId(rawKey);
        if (version == -1) {
            version = schemaMan_->getNewestTagSchemaVer(spaceId, tagId).value();
        }
        schema = schemaMan_->getTagSchema(spaceId, tagId, version);
    } else if (NebulaKeyUtils::isEdge(key)) {
        EdgeType edgeTypeId = NebulaKeyUtils::getEdgeType(rawKey);
        if (version == -1) {
            version = schemaMan_->getNewestEdgeSchemaVer(spaceId, edgeTypeId).value();
        }
        schema = schemaMan_->getEdgeSchema(spaceId, edgeTypeId, version);
    } else {
        LOG(ERROR) << "Key Type Error : " << key;
    }
    return schema;
}


std::string HBaseStore::encode(GraphSpaceID spaceId,
                               const std::string& key,
                               KVMap& data) {
    std::string version = data[kSchemaVersionColumnName];
    auto schema = this->getSchema(spaceId, key, folly::to<SchemaVer>(version));
    std::vector<dataman::Value> values;
    for (size_t index = 0; index < schema->getNumFields(); index++) {
        auto fieldName = schema->getFieldName(index);
        if (UNLIKELY(nullptr == fieldName)) {
            return "";
        }
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


std::vector<KV> HBaseStore::decode(GraphSpaceID spaceId,
                                   const std::string& key,
                                   std::string& encoded) {
    auto schema = this->getSchema(spaceId, key);
    dataman::NebulaCodecImpl codec;
    auto result = codec.decode(encoded, schema).value();
    std::vector<KV> data;
    auto version = schema->getVersion();
    data.emplace_back(kSchemaVersionColumnName, folly::to<std::string>(version));
    for (size_t index = 0; index < schema->getNumFields(); index++) {
        auto fieldName = schema->getFieldName(index);
        if (UNLIKELY(nullptr == fieldName)) {
            return std::vector<KV>();
        }
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


ResultCode HBaseStore::range(GraphSpaceID spaceId,
                             const std::string& start,
                             const std::string& end,
                             std::unique_ptr<KVIterator>* storageIter) {
    auto tableName = this->spaceIdToTableName(spaceId);
    std::vector<std::pair<std::string, KVMap>> dataList;
    auto startRowKey = this->getRowKey(start);
    auto endRowKey = this->getRowKey(end);
    ResultCode code = client_->range(tableName, startRowKey, endRowKey, dataList);
    std::vector<KV> keyValues;
    for (auto& data : dataList) {
        std::string rowKey = data.first;
        std::string key;
        key.reserve(sizeof(PartitionID) + rowKey.size());
        key.append(reinterpret_cast<const char*>(&kFillPartId), sizeof(PartitionID))
           .append(rowKey);
        auto value = this->encode(spaceId, key, data.second);
        keyValues.emplace_back(std::make_pair(key, value));
    }
    storageIter->reset(new HBaseRangeIter(keyValues.begin(), keyValues.end()));
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Range Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseStore::prefix(GraphSpaceID spaceId,
                              const std::string& prefix,
                              std::unique_ptr<KVIterator>* storageIter) {
    auto tableName = this->spaceIdToTableName(spaceId);
    std::string startRowKey, endRowKey;
    startRowKey.reserve(kMaxRowKeyLength);
    endRowKey.reserve(kMaxRowKeyLength);
    startRowKey.append(prefix);
    endRowKey.append(prefix);
    for (size_t n = 0; n < kMaxRowKeyLength - prefix.size(); n++) {
        startRowKey.append(reinterpret_cast<const char*>(&kFillMin), sizeof(uint8_t));
        endRowKey.append(reinterpret_cast<const char*>(&kFillMax), sizeof(uint8_t));
    }
    ResultCode code = this->range(spaceId, startRowKey, endRowKey, storageIter);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Prefix " << prefix << " Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseStore::multiRemove(GraphSpaceID spaceId,
                                   std::vector<std::string>& keys) {
    auto tableName = this->spaceIdToTableName(spaceId);
    std::vector<std::string> rowKeys;
    for (size_t i = 0; i < keys.size(); i++) {
        auto rowKey = this->getRowKey(keys[i]);
        rowKeys.emplace_back(rowKey);
    }
    ResultCode code = client_->multiRemove(tableName, rowKeys);
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "MultiRemove Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseStore::get(GraphSpaceID spaceId,
                           PartitionID partId,
                           const std::string& key,
                           std::string* value) {
    UNUSED(partId);
    auto tableName = this->spaceIdToTableName(spaceId);
    auto rowKey = this->getRowKey(key);
    KVMap data;
    ResultCode code = client_->get(tableName, rowKey, data);
    if (code == ResultCode::SUCCEEDED) {
        *value = this->encode(spaceId, key, data);
    } else if (code == ResultCode::ERR_KEY_NOT_FOUND) {
        LOG(ERROR) << "Get: " << key << " Not Found";
    } else if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "Get Failed: " << key << ", the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseStore::multiGet(GraphSpaceID spaceId,
                                PartitionID partId,
                                const std::vector<std::string>& keys,
                                std::vector<std::string>* values) {
    UNUSED(partId);
    auto tableName = this->spaceIdToTableName(spaceId);
    std::vector<std::string> rowKeys;
    for (auto& key : keys) {
        auto rowKey = this->getRowKey(key);
        rowKeys.emplace_back(rowKey);
    }
    std::vector<std::pair<std::string, KVMap>> dataList;
    ResultCode code = client_->multiGet(tableName, rowKeys, dataList);
    for (size_t index = 0; index < dataList.size(); index++) {
        auto value = this->encode(spaceId, keys[index], dataList[index].second);
        values->emplace_back(value);
    }
    if (code == ResultCode::ERR_IO_ERROR) {
        LOG(ERROR) << "MultiGet Failed: the HBase I/O error.";
    }
    return code;
}


ResultCode HBaseStore::range(GraphSpaceID spaceId,
                             PartitionID partId,
                             const std::string& start,
                             const std::string& end,
                             std::unique_ptr<KVIterator>* iter) {
    UNUSED(partId);
    return this->range(spaceId, start, end, iter);
}


ResultCode HBaseStore::prefix(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& prefix,
                              std::unique_ptr<KVIterator>* iter) {
    UNUSED(partId);
    return this->prefix(spaceId, prefix, iter);
}


void HBaseStore::asyncMultiPut(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::vector<KV> keyValues,
                               KVCallback cb) {
    UNUSED(partId);
    auto multiPut = [this, &spaceId, &keyValues] () -> ResultCode {
        auto tableName = this->spaceIdToTableName(spaceId);
        std::vector<std::pair<std::string, std::vector<KV>>> dataList;
        for (size_t i = 0; i < keyValues.size(); i++) {
            auto rowKey = this->getRowKey(keyValues[i].first);
            auto data = this->decode(spaceId, keyValues[i].first, keyValues[i].second);
            dataList.emplace_back(std::make_pair(rowKey, data));
        }
        ResultCode code = client_->multiPut(tableName, dataList);
        if (code == ResultCode::ERR_IO_ERROR) {
            LOG(ERROR) << "MultiPut Failed: the HBase I/O error.";
        }
        return code;
    };
    return cb(multiPut());
}


void HBaseStore::asyncRemove(GraphSpaceID spaceId,
                             PartitionID partId,
                             const std::string& key,
                             KVCallback cb) {
    UNUSED(partId);
    auto remove = [this, &spaceId, &key] () -> ResultCode {
        auto tableName = this->spaceIdToTableName(spaceId);
        auto rowKey = this->getRowKey(key);
        ResultCode code = client_->remove(tableName, rowKey);
        if (code == ResultCode::ERR_IO_ERROR) {
            LOG(ERROR) << "Remove Failed: " << key << ", the HBase I/O error.";
        }
        return code;
    };
    return cb(remove());
}


void HBaseStore::asyncMultiRemove(GraphSpaceID spaceId,
                                  PartitionID  partId,
                                  std::vector<std::string> keys,
                                  KVCallback cb) {
    UNUSED(partId);
    return cb(this->multiRemove(spaceId, keys));
}


void HBaseStore::asyncRemoveRange(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const std::string& start,
                                  const std::string& end,
                                  KVCallback cb) {
    UNUSED(partId);
    auto removeRange = [this, &spaceId, &start, &end] () -> ResultCode {
        std::unique_ptr<kvstore::KVIterator> iter;
        ResultCode rangeCode = this->range(spaceId, start, end, &iter);
        if (rangeCode == ResultCode::SUCCEEDED) {
            std::vector<std::string> keys;
            while (iter->valid()) {
                keys.emplace_back(iter->key());
                iter->next();
            }
            ResultCode code = this->multiRemove(spaceId, keys);
            if (code == ResultCode::ERR_IO_ERROR) {
                LOG(ERROR) << "RemoveRange Failed: the HBase I/O error.";
            }
            return code;
        } else if (rangeCode == ResultCode::ERR_IO_ERROR) {
            LOG(ERROR) << "RemoveRange Failed on Range: the HBase I/O error.";
        }
        return rangeCode;
    };
    return cb(removeRange());
}


void HBaseStore::asyncRemovePrefix(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const std::string& prefix,
                                   KVCallback cb) {
    UNUSED(partId);
    auto removePrefix = [this, &spaceId, &prefix] () -> ResultCode {
        std::unique_ptr<kvstore::KVIterator> iter;
        ResultCode prefixCode = this->prefix(spaceId, prefix, &iter);
        if (prefixCode == ResultCode::SUCCEEDED) {
            std::vector<std::string> keys;
            while (iter->valid()) {
                keys.emplace_back(iter->key());
                iter->next();
            }
            ResultCode code = this->multiRemove(spaceId, keys);
            if (code == ResultCode::ERR_IO_ERROR) {
                LOG(ERROR) << "RemoveRange Failed: the HBase I/O error.";
            }
            return code;
        } else if (prefixCode == ResultCode::ERR_IO_ERROR) {
            LOG(ERROR) << "RemoveRange Failed on Range: the HBase I/O error.";
        }
        return prefixCode;
    };
    return cb(removePrefix());
}

ResultCode HBaseStore::ingest(GraphSpaceID spaceId) {
    UNUSED(spaceId);
    return ResultCode::ERR_UNSUPPORTED;
}

}  // namespace kvstore
}  // namespace nebula

