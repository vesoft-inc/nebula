/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_LOGENCODER_H_
#define KVSTORE_LOGENCODER_H_

#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {

enum LogType : char {
    OP_PUT            = 0x1,
    OP_MULTI_PUT      = 0x2,
    OP_REMOVE         = 0x3,
    OP_MULTI_REMOVE   = 0x4,
    OP_REMOVE_RANGE   = 0x6,
    OP_ADD_LEARNER    = 0x07,
    OP_TRANS_LEADER   = 0x08,
    OP_ADD_PEER       = 0x09,
    OP_REMOVE_PEER    = 0x10,
    OP_BATCH_WRITE    = 0x11,
};

enum BatchLogType : char {
    OP_BATCH_PUT            = 0x1,
    OP_BATCH_REMOVE         = 0x2,
    OP_BATCH_REMOVE_RANGE   = 0x3,
};

std::string encodeKV(const folly::StringPiece& key,
                     const folly::StringPiece& val);

std::pair<folly::StringPiece, folly::StringPiece> decodeKV(const std::string& data);

std::string encodeSingleValue(LogType type, folly::StringPiece val);
folly::StringPiece decodeSingleValue(folly::StringPiece encoded);

std::string encodeMultiValues(LogType type, const std::vector<std::string>& values);
std::string encodeMultiValues(LogType type, const std::vector<KV>& kvs);
std::string encodeMultiValues(LogType type,
                              folly::StringPiece v1,
                              folly::StringPiece v2);
std::vector<folly::StringPiece> decodeMultiValues(folly::StringPiece encoded);

std::string
encodeBatchValue(const std::vector<std::tuple<BatchLogType, std::string, std::string>>& batch);

std::vector<std::pair<BatchLogType, std::pair<folly::StringPiece, folly::StringPiece>>>
decodeBatchValue(folly::StringPiece encoded);

std::string encodeHost(LogType type, const HostAddr& learner);
HostAddr decodeHost(LogType type, const folly::StringPiece& encoded);

int64_t getTimestamp(const folly::StringPiece& command);


class BatchHolder {
public:
    BatchHolder() = default;
    ~BatchHolder() = default;

    void put(std::string&& key, std::string&& val) {
        auto op = std::make_tuple(BatchLogType::OP_BATCH_PUT,
                                  std::forward<std::string>(key),
                                  std::forward<std::string>(val));
        batch_.emplace_back(std::move(op));
    }

    void remove(std::string&& key) {
        auto op = std::make_tuple(BatchLogType::OP_BATCH_REMOVE,
                                  std::forward<std::string>(key),
                                  "");
        batch_.emplace_back(std::move(op));
    }

    void rangeRemove(std::string&& begin, std::string&& end) {
        auto op = std::make_tuple(BatchLogType::OP_BATCH_REMOVE_RANGE,
                                  std::forward<std::string>(begin),
                                  std::forward<std::string>(end));
        batch_.emplace_back(std::move(op));
    }

    void clear() {
        batch_.clear();
    }

    const std::vector<std::tuple<BatchLogType, std::string, std::string>>& getBatch() {
        return batch_;
    }

private:
    std::vector<std::tuple<BatchLogType, std::string, std::string>> batch_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LOGENCODER_H_
