/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaUtils.h"
#include <string.h>
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

std::string MetaUtils::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(256);
    key.append(kSpacesTable.data(), kSpacesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaUtils::spaceVal(int32_t partsNum, int32_t replicaFactor, const std::string& name) {
    std::string val;
    val.reserve(256);
    val.append(reinterpret_cast<const char*>(&partsNum), sizeof(partsNum));
    val.append(reinterpret_cast<const char*>(&replicaFactor), sizeof(replicaFactor));
    val.append(name);
    return val;
}

const std::string& MetaUtils::spacePrefix() {
    return kSpacesTable;
}

GraphSpaceID MetaUtils::spaceId(folly::StringPiece rawKey) {
    return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + kSpacesTable.size());
}

folly::StringPiece MetaUtils::spaceName(folly::StringPiece rawVal) {
    return rawVal.subpiece(sizeof(int32_t)*2);
}

std::string MetaUtils::partKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    key.reserve(128);
    key.append(kPartsTable.data(), kPartsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaUtils::partVal(const std::vector<nebula::cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(128);
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip));
        val.append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(128);
    prefix.append(kPartsTable.data(), kPartsTable.size());
    prefix.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::vector<nebula::cpp2::HostAddr> MetaUtils::parsePartVal(folly::StringPiece val) {
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

std::string MetaUtils::hostKey(IPv4 ip, Port port) {
    std::string key;
    key.reserve(128);
    key.append(kHostsTable.data(), kHostsTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaUtils::hostVal() {
    return "";
}

const std::string& MetaUtils::hostPrefix() {
    return kHostsTable;
}

nebula::cpp2::HostAddr MetaUtils::parseHostKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

std::string MetaUtils::schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, int64_t version) {
    std::string key;
    key.reserve(128);
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    key.append(reinterpret_cast<const char*>(&version), sizeof(version));
    return key;
}

std::string MetaUtils::schemaEdgeVal(nebula::cpp2::Schema schema) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(schema, &val);
    return val;
}

std::string MetaUtils::schemaTagKey(GraphSpaceID spaceId, TagID tagId, int64_t version) {
    std::string key;
    key.reserve(128);
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    key.append(reinterpret_cast<const char*>(&version), sizeof(version));
    return key;
}

std::string MetaUtils::schemaTagVal(nebula::cpp2::Schema schema) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(schema, &val);
    return val;
}

nebula::cpp2::Schema MetaUtils::parseSchema(folly::StringPiece rawData) {
    nebula::cpp2::Schema schema;
    apache::thrift::CompactSerializer::deserialize(rawData, schema);
    return schema;
}

std::string MetaUtils::indexKey(EntryType type, const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(name);
    return key;
}

}  // namespace meta
}  // namespace nebula
