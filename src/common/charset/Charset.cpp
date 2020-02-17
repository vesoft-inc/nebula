/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "charset/Charset.h"

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
                                        const std::string& p1,
                                        const std::string& p2) {
    auto iter = collationToLocale_.find(collateName);
    if (iter == collationToLocale_.end()) {
        return Status::Error("Collation `%s' not support", collateName.c_str());
    }

    // For charset is incomplete in locale
    std::locale loc(iter->second);
    if (!std::has_facet<std::ctype<char>>(loc)) {
        return Status::Error("Locale: `%s' environment is not supported", iter->second.c_str());
    }
    auto& f = std::use_facet<std::collate<char>>(loc);

    std::string str1(p1), str2(p2);
    return f.compare(&str1[0], &str1[0] + str1.size(),
                     &str2[0], &str2[0] + str2.size());
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
