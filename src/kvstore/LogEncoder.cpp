/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "time/WallClock.h"
#include "kvstore/LogEncoder.h"

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

std::string encodeHost(LogType type, const HostAddr& host) {
    std::string encoded;
    encoded.reserve(sizeof(int64_t) + 1 + sizeof(HostAddr));
    // Timestamp (8 bytes)
    int64_t ts = time::WallClock::fastNowInMilliSec();
    encoded.append(reinterpret_cast<char*>(&ts), sizeof(int64_t));
    // Log type
    encoded.append(reinterpret_cast<char*>(&type), 1);
    encoded.append(reinterpret_cast<const char*>(&host), sizeof(HostAddr));
    return encoded;
}

HostAddr decodeHost(LogType type, folly::StringPiece encoded) {
    HostAddr addr;
    CHECK_EQ(sizeof(int64_t) + 1 + sizeof(HostAddr), encoded.size());
    CHECK(encoded[sizeof(int64_t)] == type);
    memcpy(&addr.first, encoded.begin() + sizeof(int64_t) + 1, sizeof(addr.first));
    memcpy(&addr.second,
           encoded.begin() + sizeof(int64_t) + 1 + sizeof(addr.first),
           sizeof(addr.second));
    return addr;
}

}  // namespace kvstore
}  // namespace nebula

