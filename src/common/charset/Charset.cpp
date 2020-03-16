/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "charset/Charset.h"
#include "charset/UniUtf8.h"

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


size_t CharsetInfo::getUtf8Charlength(const std::string& str) {
    size_t length = 0;
    for (size_t i = 0, len = 0; i < str.length(); i += len) {
        unsigned char byte = str[i];
        if (byte >= 0xFC) {
            len = 6;
        } else if (byte >= 0xF8) {
            len = 5;
        } else if (byte >= 0xF0) {
            len = 4;
        } else if (byte >= 0xE0) {
            len = 3;
        } else if (byte >= 0xC0) {
            len = 2;
        } else {
            len = 1;
        }
        length++;
    }
    return length;
}


StatusOr<int> CharsetInfo::nebulaStrCmp(const std::string& collateName,
                                        const std::string& str1,
                                        const std::string& str2) {
    int ret = 0;
    if (!collateName.compare("utf8_bin") || !collateName.compare("utf8mb4_bin")) {
        ret = str1.compare(str2);
    } else if (!collateName.compare("utf8_general_ci") ||
               !collateName.compare("utf8mb4_general_ci")) {
        ret = UniUtf8::utf8GeneralCICmp(str1, str2);
    }
    if (ret < 0) {
        ret = -1;
    } else if (ret > 0) {
        ret = 1;
    }
    return ret;
}


StatusOr<bool> CharsetInfo::nebulaStrCmpLT(const std::string& collateName,
                                           const std::string& p1,
                                           const std::string& p2) {
    auto ret = nebulaStrCmp(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    auto val = ret.value();
    return val < 0;
}


StatusOr<bool> CharsetInfo:: nebulaStrCmpLE(const std::string& collateName,
                                            const std::string& p1,
                                            const std::string& p2) {
    auto ret = nebulaStrCmp(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    auto val = ret.value();
    return val <= 0;
}


StatusOr<bool> CharsetInfo::nebulaStrCmpGT(const std::string& collateName,
                                           const std::string& p1,
                                           const std::string& p2) {
    auto ret = nebulaStrCmpLE(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    return !ret.value();
}


StatusOr<bool> CharsetInfo::nebulaStrCmpGE(const std::string& collateName,
                                           const std::string& p1,
                                           const std::string& p2) {
    auto ret = nebulaStrCmpLT(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    return !ret.value();
}


StatusOr<bool> CharsetInfo::nebulaStrCmpEQ(const std::string& collateName,
                                           const std::string& p1,
                                           const std::string& p2) {
    auto ret = nebulaStrCmp(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    return ret.value() == 0;
}


StatusOr<bool> CharsetInfo::nebulaStrCmpNE(const std::string& collateName,
                                           const std::string& p1,
                                           const std::string& p2) {
    auto ret = nebulaStrCmpEQ(collateName, p1, p2);
    if (!ret.ok()) {
        return ret.status();
    }
    return !ret.value();
}

}   // namespace nebula
