/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/charset/Charset.h"

namespace nebula {

Status CharsetInfo::isSupportCharset(const std::string& charsetName) {
    if (supportCharsets_.find(charsetName) == supportCharsets_.end()) {
        return Status::Error("Charset `%s' not support", charsetName.c_str());
    }
    return Status::OK();
}


Status CharsetInfo::isSupportCollate(const std::string& collateName) {
    if (supportCollations_.find(collateName) == supportCollations_.end()) {
        return Status::Error("Collation `%s' not support", collateName.c_str());
    }
    return Status::OK();
}


Status CharsetInfo::charsetAndCollateMatch(const std::string& charsetName,
                                           const std::string& collateName) {
    auto iter = charsetDesc_.find(charsetName);
    if (iter != charsetDesc_.end()) {
        for (auto& sc : iter->second.supportColls_) {
            if (!sc.compare(collateName)) {
                return Status::OK();
            }
        }
    }
    return  Status::Error("Charset `%s' and Collation `%s' not match",
                          charsetName.c_str(), collateName.c_str());
}


StatusOr<std::string> CharsetInfo::getDefaultCollationbyCharset(const std::string& charsetName) {
    auto iter = charsetDesc_.find(charsetName);
    if (iter != charsetDesc_.end()) {
        return iter->second.defaultColl_;
    }
    return Status::Error("Charset `%s' not support", charsetName.c_str());
}


StatusOr<std::string> CharsetInfo::getCharsetbyCollation(const std::string& collationName ) {
    for (auto& cset : charsetDesc_) {
        for (auto& coll : cset.second.supportColls_) {
            if (!coll.compare(collationName)) {
                return cset.first;
            }
        }
    }
    return Status::Error("Collation `%s' not support", collationName.c_str());
}

}   // namespace nebula
