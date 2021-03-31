/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/WallClock.h"
#include "common/datatypes/HostAddrOps.inl"
#include "kvstore/LogEncoder.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace kvstore {

constexpr auto kHeadLen = sizeof(int64_t) + 1 + sizeof(uint32_t);

std::string encodeKV(const folly::StringPiece& key,
                     const folly::StringPiece& val) {
    uint32_t ksize = key.size();
    uint32_t vsize = val.size();
    std::string str;
    str.reserve(sizeof(uint32_t) * 2 + ksize + vsize);
    str.append(reinterpret_cast<const char*>(&ksize), sizeof(ksize));
    str.append(reinterpret_cast<const char*>(&vsize), sizeof(vsize));
    str.append(key.data(), ksize);
    str.append(val.data(), vsize);
    return str;
}

std::pair<folly::StringPiece, folly::StringPiece> decodeKV(const std::string& data) {
    auto ksize = *reinterpret_cast<const uint32_t*>(data.data());
    auto vsize = *reinterpret_cast<const uint32_t*>(data.data() + sizeof(ksize));
    auto key = folly::StringPiece(data.data() + sizeof(ksize) + sizeof(vsize), ksize);
    auto val = folly::StringPiece(data.data() + sizeof(ksize) + sizeof(vsize) + ksize, vsize);
    return std::make_pair(key, val);
}

std::string encodeSingleValue(LogType type, folly::StringPiece val) {
    std::string encoded;
    encoded.reserve(val.size() + kHeadLen);
    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    // Value length
    uint32_t len = static_cast<uint32_t>(val.size());
    encoded.append(reinterpret_cast<char*>(&len), sizeof(len));
    // Value
    encoded.append(val.begin(), val.size());

    return encoded;
}


folly::StringPiece decodeSingleValue(folly::StringPiece encoded) {
    // Skip the timestamp and the first type byte
    auto* p = encoded.begin() + sizeof(int64_t) + 1;
    uint32_t len = *(reinterpret_cast<const uint32_t*>(p));
    DCHECK_EQ(len + kHeadLen, encoded.size());
    return folly::StringPiece(p + sizeof(uint32_t), len);
}


std::string encodeMultiValues(LogType type, const std::vector<std::string>& values) {
    size_t totalLen = 0;
    for (auto& v : values) {
        totalLen += (sizeof(uint32_t) + v.size());
    }

    std::string encoded;
    encoded.reserve(totalLen + kHeadLen);

    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    // Number of values
    uint32_t num = values.size();
    encoded.append(reinterpret_cast<char*>(&num), sizeof(uint32_t));
    // Values
    for (auto& v : values) {
        uint32_t len = v.size();
        encoded.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        encoded.append(v.data(), len);
    }

    return encoded;
}


std::string encodeMultiValues(LogType type, const std::vector<KV>& kvs) {
    size_t totalLen = 0;
    for (auto& kv : kvs) {
        totalLen += (2 * sizeof(uint32_t) + kv.first.size() + kv.second.size());
    }

    std::string encoded;
    encoded.reserve(totalLen + kHeadLen);

    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    // Number of total strings: #keys + #values
    uint32_t num = 2 * kvs.size();
    encoded.append(reinterpret_cast<char*>(&num), sizeof(uint32_t));
    // Key/value pairs
    for (auto& kv : kvs) {
        uint32_t len = kv.first.size();
        encoded.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        encoded.append(kv.first.data(), len);
        len = kv.second.size();
        encoded.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        encoded.append(kv.second.data(), len);
    }

    return encoded;
}


std::string encodeMultiValues(LogType type,
                              folly::StringPiece v1,
                              folly::StringPiece v2) {
    std::string encoded;
    encoded.reserve(kHeadLen + 2 * sizeof(uint32_t) + v1.size() + v2.size());

    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    // Number of values
    uint32_t num = 2;
    encoded.append(reinterpret_cast<char*>(&num), sizeof(uint32_t));
    // Values
    uint32_t len = v1.size();
    encoded.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
    encoded.append(v1.begin(), len);
    len = v2.size();
    encoded.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
    encoded.append(v2.begin(), len);

    return encoded;
}


std::vector<folly::StringPiece> decodeMultiValues(folly::StringPiece encoded) {
    // Skip the timestamp and the first type byte
    auto* p = encoded.begin() + sizeof(int64_t) + 1;
    uint32_t numValues = *(reinterpret_cast<const uint32_t*>(p));

    std::vector<folly::StringPiece> values;
    p += sizeof(uint32_t);
    for (auto i = 0U; i < numValues; i++) {
        uint32_t len = *(reinterpret_cast<const uint32_t*>(p));
        DCHECK_LE(p + sizeof(uint32_t) + len, encoded.begin() + encoded.size());
        values.emplace_back(p + sizeof(uint32_t), len);
        p += (sizeof(uint32_t) + len);
    }
    DCHECK_EQ(p, encoded.begin() + encoded.size());

    return values;
}

std::string
encodeBatchValue(const std::vector<std::tuple<BatchLogType, std::string, std::string>>& batch) {
    auto type = LogType::OP_BATCH_WRITE;
    std::string encoded;
    encoded.reserve(1024);

    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    // Number of values
    auto num = static_cast<uint32_t>(batch.size());
    encoded.append(reinterpret_cast<char*>(&num), sizeof(uint32_t));
    // Values
    for (auto& op : batch) {
        auto opType = std::get<0>(op);
        auto key = std::get<1>(op);
        auto val = std::get<2>(op);
        auto keySize = key.size();
        auto valSize = val.size();
        encoded.append(reinterpret_cast<char*>(&opType), 1)
               .append(reinterpret_cast<char*>(&keySize), sizeof(uint32_t))
               .append(key.data(), keySize)
               .append(reinterpret_cast<char*>(&valSize), sizeof(uint32_t))
               .append(val.data(), valSize);
    }

    return encoded;
}

std::vector<std::pair<BatchLogType, std::pair<folly::StringPiece, folly::StringPiece>>>
decodeBatchValue(folly::StringPiece encoded) {
    // Skip the timestamp and the first type byte
    auto* p = encoded.begin() + sizeof(int64_t) + 1;
    uint32_t numValues = *(reinterpret_cast<const uint32_t*>(p));
    p += sizeof(uint32_t);
    std::vector<std::pair<BatchLogType, std::pair<folly::StringPiece, folly::StringPiece>>> batch;
    for (auto i = 0U; i < numValues; i++) {
        auto offset = 0;
        BatchLogType type = *(reinterpret_cast<const BatchLogType *>(p));
        p += sizeof(LogType);
        uint32_t len1 = *(reinterpret_cast<const uint32_t*>(p));
        offset += sizeof(uint32_t) + len1;
        uint32_t len2 = *(reinterpret_cast<const uint32_t*>(p + offset));
        offset += sizeof(uint32_t);
        batch.emplace_back(type, std::pair<folly::StringPiece, folly::StringPiece>
                (folly::StringPiece(p + sizeof(uint32_t), len1),
                 folly::StringPiece(p + offset, len2)));
        p += offset + len2;
    }
    return batch;
}

std::string encodeHost(LogType type, const HostAddr& host) {
    std::string encoded;
    // 15 refers to "255.255.255.255"
    encoded.reserve(sizeof(int64_t) + 1 + 15 + sizeof(int));
    int64_t ts = time::WallClock::fastNowInMilliSec();
    std::string encodedHost;
    apache::thrift::CompactSerializer::serialize(host, &encodedHost);

    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t))
           .append(reinterpret_cast<char*>(&type), 1)
           .append(encodedHost);
    return encoded;
}

HostAddr decodeHost(LogType type, const folly::StringPiece& encoded) {
    HostAddr addr;

    CHECK_GE(encoded.size(), sizeof(int64_t) + 1 + sizeof(size_t) + sizeof(Port));
    CHECK(encoded[sizeof(int64_t)] == type);

    folly::StringPiece raw = encoded;
    raw.advance(sizeof(int64_t) + 1);

    HostAddr host;
    apache::thrift::CompactSerializer::deserialize(raw, host);
    return host;
}

int64_t getTimestamp(const folly::StringPiece& command) {
    return *reinterpret_cast<const int64_t*>(command.begin());
}

}  // namespace kvstore
}  // namespace nebula

