/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/Part.h"

namespace nebula {
namespace kvstore {

void SimplePart::asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) {
    CHECK_NOTNULL(engine_);
    auto ret = engine_->multiPut(std::move(keyValues));
    cb(ret, HostAddr(0, 0));
}

void SimplePart::asyncRemove(const std::string& key, KVCallback cb) {
    CHECK_NOTNULL(engine_);
    auto ret = engine_->remove(key);
    cb(ret, HostAddr(0, 0));
}

void SimplePart::asyncMultiRemove(std::vector<std::string> keys, KVCallback cb) {
    CHECK_NOTNULL(engine_);
    auto ret = engine_->multiRemove(std::move(keys));
    cb(ret, HostAddr(0, 0));
}

void SimplePart::asyncRemoveRange(const std::string& start,
                                  const std::string& end,
                                  KVCallback cb) {
    CHECK_NOTNULL(engine_);
    auto ret = engine_->removeRange(start, end);
    cb(ret, HostAddr(0, 0));
}

}  // namespace kvstore
}  // namespace nebula

