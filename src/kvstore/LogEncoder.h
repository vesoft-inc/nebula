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
    OP_REMOVE_PREFIX  = 0x5,
    OP_REMOVE_RANGE   = 0x6,
    OP_ADD_LEARNER    = 0x07,
    OP_TRANS_LEADER   = 0x08,
    OP_ADD_PEER       = 0x09,
    OP_REMOVE_PEER    = 0x10,
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


std::string encodeHost(LogType type, const HostAddr& learner);
HostAddr decodeHost(LogType type, const folly::StringPiece& encoded);

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LOGENCODER_H_
