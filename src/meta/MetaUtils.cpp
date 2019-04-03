/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaUtils.h"
#include "proxygen/lib/utils/CryptUtil.h"
#include <string.h>

namespace nebula {
namespace meta {

const char kSpacesTable[] = "__spaces__";
const char kPartsTable[] = "__parts__";
const char kHostsTable[] = "__hosts__";
const std::string kUserTable  = "__user__";   // NOLINT

std::string MetaUtils::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    static const size_t prefixLen = ::strlen(kSpacesTable);
    key.reserve(prefixLen + sizeof(spaceId));
    key.append(kSpacesTable, prefixLen);
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaUtils::spaceVal(int32_t partsNum, int32_t replicaFactor, const std::string& name) {
    std::string val;
    val.reserve(sizeof(partsNum) + sizeof(replicaFactor) + name.size());
    val.append(reinterpret_cast<const char*>(&partsNum), sizeof(partsNum));
    val.append(reinterpret_cast<const char*>(&replicaFactor), sizeof(replicaFactor));
    val.append(name);
    return val;
}

const std::string& MetaUtils::spacePrefix() {
    static const std::string prefix = kSpacesTable;
    return prefix;
}

GraphSpaceID MetaUtils::spaceId(folly::StringPiece rawKey) {
    static const size_t prefixLen = ::strlen(kSpacesTable);
    return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + prefixLen);
}

folly::StringPiece MetaUtils::spaceName(folly::StringPiece rawVal) {
    return rawVal.subpiece(sizeof(int32_t)*2);
}

std::string MetaUtils::partKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    static constexpr size_t tableLen = ::strlen(kPartsTable);
    key.reserve(tableLen + sizeof(GraphSpaceID) + sizeof(PartitionID));
    key.append(kPartsTable, tableLen);
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaUtils::partVal(const std::vector<nebula::cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(hosts.size() * sizeof(int32_t) * 2);
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip));
        val.append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    static constexpr size_t tableLen = ::strlen(kPartsTable);
    prefix.reserve(tableLen + sizeof(GraphSpaceID));
    prefix.append(kPartsTable, tableLen);
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
    static constexpr size_t tableLen = ::strlen(kHostsTable);
    key.reserve(tableLen + sizeof(IPv4) + sizeof(Port));
    key.append(kHostsTable, tableLen);
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaUtils::hostVal() {
    return "";
}

const std::string& MetaUtils::hostPrefix() {
    static const std::string prefix = kHostsTable;
    return prefix;
}

nebula::cpp2::HostAddr MetaUtils::parseHostKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    static constexpr size_t tableLen = ::strlen(kHostsTable);
    memcpy(&host, key.data() + tableLen, sizeof(host));
    return host;
}

std::string MetaUtils::schemaEdgeKey(EdgeType edgeType, int32_t version) {
    UNUSED(edgeType); UNUSED(version);
    return "";
}

std::string MetaUtils::schemaEdgeVal(nebula::cpp2::Schema schema) {
    UNUSED(schema);
    return "";
}

std::string MetaUtils::schemaTagKey(TagID tagId, int32_t version) {
    UNUSED(tagId); UNUSED(version);
    return "";
}

std::string MetaUtils::schemaTagVal(nebula::cpp2::Schema schema) {
    UNUSED(schema);
    return "";
}

std::string MetaUtils::userKey(GraphSpaceID spaceId, const std::string& name) {
    std::string key;
    key.reserve(kUserTable.size() + sizeof(GraphSpaceID) + name.length());
    key.append(kUserTable.data(), kUserTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(name);
    return key;
}

std::string MetaUtils::userVal(RoleType type, const std::string& pwd) {
    std::string val;
    std::string pwdSha = MetaUtils::encPassword(pwd);
    val.reserve(sizeof(RoleType) + pwd.size());
    val.append(reinterpret_cast<const char*>(&type), sizeof(RoleType));
    val.append(pwdSha);
    return val;
}

std::string MetaUtils::userPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(kUserTable.size() + sizeof(GraphSpaceID));
    prefix.append(kUserTable.data(), kUserTable.size());
    prefix.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::string MetaUtils::encPassword(const std::string &pwd) {
    return proxygen::md5Encode(folly::ByteRange(
                    reinterpret_cast<const unsigned char*>(pwd.c_str()),
                    pwd.length()));
}

nebula::cpp2::UserItem MetaUtils::parseUserItem(const std::string& key, const std::string& val) {
    auto prefixLen = kUserTable.size() + sizeof(GraphSpaceID);
    auto type = *reinterpret_cast<const nebula::cpp2::RoleType*>(val.data());
    auto pwd = val.substr(sizeof(RoleType), val.size() - sizeof(RoleType));
    auto name = key.substr(prefixLen, key.size() - prefixLen);
    nebula::cpp2::UserItem userItem(apache::thrift::FragileConstructor::FRAGILE,
            std::move(name), std::move(pwd), std::move(type));
    return userItem;
}

}  // namespace meta
}  // namespace nebula
