/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"

namespace nebula {

// static
bool NebulaKeyUtils::isValidVidLen(size_t vIdLen, VertexID srcVId, VertexID dstVId) {
    if (srcVId.size() > vIdLen || dstVId.size() > vIdLen) {
        return false;
    }
    return true;
}

// static
std::string NebulaKeyUtils::vertexKey(size_t vIdLen,
                                      PartitionID partId,
                                      VertexID vId,
                                      TagID tagId,
                                      TagVersion tv) {
    CHECK_GE(vIdLen, vId.size());
    tagId &= kTagMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen + vIdLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0')
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&tv), sizeof(TagVersion));
    return key;
}

// static
std::string NebulaKeyUtils::edgeKey(size_t vIdLen,
                                    PartitionID partId,
                                    VertexID srcId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexID dstId,
                                    EdgeVersion ev) {
    CHECK_GE(vIdLen, srcId.size());
    CHECK_GE(vIdLen, dstId.size());
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kEdgeLen + (vIdLen << 1));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(dstId.data(), dstId.size())
       .append(vIdLen - dstId.size(), '\0')
       .append(reinterpret_cast<const char*>(&ev), sizeof(EdgeVersion));
    return key;
}

// static
std::string NebulaKeyUtils::systemCommitKey(PartitionID partId) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kSystem);
    uint32_t type = static_cast<uint32_t>(NebulaSystemKeyType::kSystemCommit);
    std::string key;
    key.reserve(kSystemLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
    return key;
}

// static
std::string NebulaKeyUtils::systemPartKey(PartitionID partId) {
    uint32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kSystem);
    uint32_t type = static_cast<uint32_t>(NebulaSystemKeyType::kSystemPart);
    std::string key;
    key.reserve(kSystemLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
    return key;
}

// static
std::string NebulaKeyUtils::uuidKey(PartitionID partId, const folly::StringPiece& name) {
    std::string key;
    key.reserve(sizeof(PartitionID) + name.size());
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kUUID);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(name.data(), name.size());
    return key;
}

// static
std::string NebulaKeyUtils::kvKey(PartitionID partId, const folly::StringPiece& name) {
    std::string key;
    key.reserve(sizeof(PartitionID) + name.size());
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(name.data(), name.size());
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(size_t vIdLen, PartitionID partId,
                                         VertexID vId, TagID tagId) {
    CHECK_GE(vIdLen, vId.size());
    tagId &= kTagMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + vIdLen + sizeof(TagID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0')
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(size_t vIdLen, PartitionID partId, VertexID vId) {
    CHECK_GE(vIdLen, vId.size());
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + vIdLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0');
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen, PartitionID partId,
                                       VertexID srcId, EdgeType type) {
    CHECK_GE(vIdLen, srcId.size());
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + vIdLen + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen, PartitionID partId, VertexID srcId) {
    CHECK_GE(vIdLen, srcId.size());
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + vIdLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0');
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen,
                                       PartitionID partId,
                                       VertexID srcId,
                                       EdgeType type,
                                       EdgeRanking rank,
                                       VertexID dstId) {
    CHECK_GE(vIdLen, srcId.size());
    CHECK_GE(vIdLen, dstId.size());
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + (vIdLen << 1) + sizeof(EdgeType) + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(dstId.data(), dstId.size())
       .append(vIdLen - dstId.size(), '\0');
    return key;
}

// static
std::string NebulaKeyUtils::partPrefix(PartitionID partId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::snapshotPrefix(PartitionID partId) {
    // snapshot of meta would be all key-value pairs
    if (partId == 0) {
        return "";
    }
    return partPrefix(partId);
}

std::string NebulaKeyUtils::systemPrefix() {
    int8_t type = static_cast<uint32_t>(NebulaKeyType::kSystem);
    std::string key;
    key.reserve(sizeof(int8_t));
    key.append(reinterpret_cast<const char*>(&type), sizeof(int8_t));
    return key;
}

std::string NebulaKeyUtils::toLockKey(const folly::StringPiece& rawKey,
                                      bool enableMvcc) {
    EdgeVersion verPlaceHolder = 0;
    EdgeVersion ver = 0;
    if (enableMvcc) {
        auto offset = rawKey.size() - sizeof(EdgeVersion);
        ver = readInt<int64_t>(rawKey.data() + offset, sizeof(EdgeVersion));
    }

    auto lockKey = keyWithNoVersion(rawKey).str();
    lockKey.append(reinterpret_cast<const char*>(&verPlaceHolder), sizeof(EdgeVersion));
    lockKey.append(reinterpret_cast<const char*>(&ver), sizeof(EdgeVersion));
    return lockKey + kLockSuffix;
}

std::string NebulaKeyUtils::toEdgeKey(const folly::StringPiece& lockKey, bool enableMvcc) {
    // edges in toss space must have edge ver greater then 0.
    EdgeVersion ver = enableMvcc ? NebulaKeyUtils::getLockVersion(lockKey) : 1;

    auto rawKey = lockWithNoVersion(lockKey).str();
    rawKey.append(reinterpret_cast<const char*>(&ver), sizeof(EdgeVersion));
    return rawKey;
}

}   // namespace nebula
