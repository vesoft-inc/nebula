/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/IndexKeyUtils.h"

namespace nebula {

// static
void IndexKeyUtils::indexRaw(const IndexValues &values, std::string& raw) {
    std::vector<int32_t> colsLen;
    for (auto& col : values) {
        if (col.first == Value::Type::STRING) {
            colsLen.emplace_back(col.second.size());
        }
        raw.append(col.second.data(), col.second.size());
    }
    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
std::string IndexKeyUtils::vertexIndexKey(size_t vIdLen, PartitionID partId,
                                          IndexID indexId, VertexID vId,
                                          const IndexValues& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    indexRaw(values, key);
    key.append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::edgeIndexKey(size_t vIdLen, PartitionID partId,
                                        IndexID indexId, VertexID srcId,
                                        EdgeRanking rank, VertexID dstId,
                                        const IndexValues& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    indexRaw(values, key);
    key.append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(dstId.data(), dstId.size())
       .append(vIdLen - dstId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::indexPrefix(PartitionID partId, IndexID indexId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(IndexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

}  // namespace nebula

