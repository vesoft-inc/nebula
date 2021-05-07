/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/EdgeKey.h"
#include "common/expression/Expression.h"

namespace nebula {

std::string EdgeKey::toString() const {
    return folly::stringPrintf(
        "%s->%s@%ld", srcid_->toString().c_str(), dstid_->toString().c_str(), rank_);
}

std::string EdgeKeys::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &key : keys_) {
        buf += key->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }

    return buf;
}

std::string EdgeKeyRef::toString() const {
    std::string buf;
    buf.reserve(256);
    if (srcid_ != nullptr) {
        buf += srcid_->toString();
    }
    if (dstid_ != nullptr) {
        buf += "->";
        buf += dstid_->toString();
    }
    if (rank_ != nullptr) {
        buf += "@";
        buf += rank_->toString();
    }
    return buf;
}

}   // namespace nebula
