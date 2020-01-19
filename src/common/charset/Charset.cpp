/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "charset/Charset.h"

namespace nebula {

std::vector<std::string> CharsetInfo::supportCharset = {"utf8"};
std::vector<std::string> CharsetInfo::supportCollation = {"utf8_bin"};

std::map<std::string, CharsetInfo::CharsetToCollation> CharsetInfo::charsetToCollation = {
    {"utf8", {"utf8", "utf8_bin", {"utf8_bin"}, "UTF-8 Unicode", 4}}
};

Status CharsetInfo::isSupportCharset(const std::string& charsetName) {
    for (auto& sc : supportCharset) {
        if (!sc.compare(charsetName)) {
            return Status::OK();
        }
    }
    return Status::Error("Charset `%s' not support", charsetName.c_str());
}


Status CharsetInfo::isSupportCollate(const std::string& collateName) {
    for (auto& sc : supportCollation) {
        if (!sc.compare(collateName)) {
                return Status::OK();
        }
    }
    return Status::Error("Collation `%s' not support", collateName.c_str());
}


Status CharsetInfo::charsetAndCollateMatch(const std::string& charsetName,
                                           const std::string& collateName) {
    auto iter = charsetToCollation.find(charsetName);
    if (iter != charsetToCollation.end()) {
        for (auto& sc : iter->second.supportColl_) {
            if (!sc.compare(collateName)) {
                return Status::OK();
            }
        }
    }
    return  Status::Error("Charset `%s' and Collation `%s' not match",
                          charsetName.c_str(), collateName.c_str());
}


StatusOr<std::string> CharsetInfo::getDefaultCollationbyCharset(const std::string& charsetName) {
    auto iter = charsetToCollation.find(charsetName);
    if (iter != charsetToCollation.end()) {
        return iter->second.defaultColl_;
    }
    return Status::Error("Charset `%s' not support", charsetName.c_str());
}


StatusOr<std::string> CharsetInfo::getCharsetbyCollation(const std::string& collationName ) {
    for (auto& cset : charsetToCollation) {
        for (auto& coll : cset.second.supportColl_) {
            if (!coll.compare(collationName)) {
                return cset.first;
            }
        }
    }
    return Status::Error("Collation `%s' not support", collationName.c_str());
}

}   // namespace nebula
