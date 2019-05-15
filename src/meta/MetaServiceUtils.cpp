/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace meta {

const std::string kSpacesTable = "__spaces__";  // NOLINT
const std::string kPartsTable  = "__parts__";   // NOLINT
const std::string kHostsTable  = "__hosts__";   // NOLINT
const std::string kTagsTable   = "__tags__";    // NOLINT
const std::string kEdgesTable  = "__edges__";   // NOLINT
const std::string kIndexTable  = "__index__";   // NOLINT

std::string MetaServiceUtils::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(128);
    key.append(kSpacesTable.data(), kSpacesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::spaceVal(int32_t partsNum,
                                       int32_t replicaFactor,
                                       const std::string& name) {
    std::string val;
    val.reserve(256);
    val.append(reinterpret_cast<const char*>(&partsNum), sizeof(partsNum));
    val.append(reinterpret_cast<const char*>(&replicaFactor), sizeof(replicaFactor));
    val.append(name);
    return val;
}

const std::string& MetaServiceUtils::spacePrefix() {
    return kSpacesTable;
}

GraphSpaceID MetaServiceUtils::spaceId(folly::StringPiece rawKey) {
    return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + kSpacesTable.size());
}

folly::StringPiece MetaServiceUtils::spaceName(folly::StringPiece rawVal) {
    return rawVal.subpiece(sizeof(int32_t)*2);
}

std::string MetaServiceUtils::partKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    key.reserve(128);
    key.append(kPartsTable.data(), kPartsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaServiceUtils::partVal(const std::vector<nebula::cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(128);
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip));
        val.append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaServiceUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(128);
    prefix.append(kPartsTable.data(), kPartsTable.size());
    prefix.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::vector<nebula::cpp2::HostAddr> MetaServiceUtils::parsePartVal(folly::StringPiece val) {
    std::vector<nebula::cpp2::HostAddr> hosts;
    static const size_t unitSize = sizeof(int32_t) * 2;
    auto hostsNum = val.size() / unitSize;
    hosts.reserve(hostsNum);
    VLOG(3) << "Total size:" << val.size()
            << ", host size:" << unitSize
            << ", host num:" << hostsNum;
    for (decltype(hostsNum) i = 0; i < hostsNum; i++) {
        nebula::cpp2::HostAddr h;
        h.set_ip(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize));
        h.set_port(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize + sizeof(int32_t)));
        hosts.emplace_back(std::move(h));
    }
    return hosts;
}

std::string MetaServiceUtils::hostKey(IPv4 ip, Port port) {
    std::string key;
    key.reserve(128);
    key.append(kHostsTable.data(), kHostsTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtils::hostVal() {
    return "";
}

const std::string& MetaServiceUtils::hostPrefix() {
    return kHostsTable;
}

nebula::cpp2::HostAddr MetaServiceUtils::parseHostKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

std::string MetaServiceUtils::schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType) {
    std::string key;
    key.reserve(128);
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    return key;
}

std::string MetaServiceUtils::schemaEdgesPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID));
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::schemaEdgeKey(GraphSpaceID spaceId,
                                            EdgeType edgeType,
                                            SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(128);
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    key.append(reinterpret_cast<const char*>(&storageVer), sizeof(storageVer));
    return key;
}

std::string MetaServiceUtils::schemaEdgeVal(const std::string& name, nebula::cpp2::Schema schema) {
    auto len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    val.append(name);
    val.append(sval);
    return val;
}

SchemaVer MetaServiceUtils::parseEdgeVersion(folly::StringPiece key) {
    auto offset = kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType);
    return std::numeric_limits<SchemaVer>::max() -
          *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtils::schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(128);
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    key.append(reinterpret_cast<const char*>(&storageVer), sizeof(version));
    return key;
}

SchemaVer MetaServiceUtils::parseTagVersion(folly::StringPiece key) {
    auto offset = kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID);
    return std::numeric_limits<SchemaVer>::max() -
          *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtils::schemaTagPrefix(GraphSpaceID spaceId, TagID tagId) {
    std::string key;
    key.reserve(128);
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    return key;
}

std::string MetaServiceUtils::schemaTagsPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID));
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::schemaTagVal(const std::string& name, nebula::cpp2::Schema schema) {
    int32_t len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    val.append(name);
    val.append(sval);
    return val;
}

nebula::cpp2::Schema MetaServiceUtils::parseSchema(folly::StringPiece rawData) {
    nebula::cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

std::string MetaServiceUtils::indexSpaceKey(const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::SPACE;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::indexTagKey(GraphSpaceID spaceId,
                                          const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::TAG;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::indexEdgeKey(GraphSpaceID spaceId,
                                           const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::EDGE;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::assembleSegmentKey(const std::string& segment,
                                                 const std::string& key) {
    std::string segmentKey;
    segmentKey.reserve(64);
    segmentKey.append(segment);
    segmentKey.append(key.data(), key.size());
    return segmentKey;
}

cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                                  const nebula::cpp2::ColumnDef col,
                                                  const cpp2::AlterSchemaOp op) {
    switch (op) {
        case cpp2::AlterSchemaOp::ADD :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(WARNING) << "Column existing : " << col.get_name();
                    return cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.emplace_back(std::move(col));
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case cpp2::AlterSchemaOp::CHANGE :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    *it = col;
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        case cpp2::AlterSchemaOp::DROP :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    cols.erase(it);
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        default :
            return cpp2::ErrorCode::E_UNKNOWN;
    }
    LOG(WARNING) << "Column not found : " << col.get_name();
    return cpp2::ErrorCode::E_NOT_FOUND;
}

}  // namespace meta
}  // namespace nebula
