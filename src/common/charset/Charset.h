/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CHARSET_CHARSET_H_
#define COMMON_CHARSET_CHARSET_H_

#include "base/Base.h"
#include "base/StatusOr.h"

namespace nebula {

struct CharsetToCollation {
    std::string               charsetName_;
    std::string               defaultColl_;
    std::vector<std::string>  supportColl_;
    std::string               desc_;
    int32_t                   maxLen_;
};


class CharsetInfo final {
public:
    static CharsetInfo* instance() {
        static std::unique_ptr<CharsetInfo> charsetInfo(new CharsetInfo());
        charsetInfo->prepare();
        return charsetInfo.get();
    }

    void prepare() {
        charsetToCollation["utf8"] = {"utf8", "utf8_bin", {"utf8_bin"}, "UTF-8 Unicode", 4};
    }

    /**
     * Check if charset is supported
     */
    Status isSupportCharset(const std::string& charsetName);

    /**
     * Check if collation is supported
     */
    Status isSupportCollate(const std::string& collateName);

    /**
     * check if charset and collation match
     */
    Status charsetAndCollateMatch(const std::string& charsetName,
                                         const std::string& collateName);

    /**
     * Get the corresponding collation according to charset
     */
    StatusOr<std::string> getDefaultCollationbyCharset(const std::string& charsetName);

    /**
     * Get the corresponding charset according to collation
     */
    StatusOr<std::string> getCharsetbyCollation(const std::string& collationName);


    std::unordered_map<std::string, CharsetToCollation> getCharsetToCollation() {
        return charsetToCollation;
    }

private:
    CharsetInfo() {}

    std::unordered_set<std::string> supportCharset = {"utf8"};

    std::unordered_set<std::string> supportCollation = {"utf8_bin"};

    std::unordered_map<std::string, CharsetToCollation> charsetToCollation;
};

}   // namespace nebula

#endif  // COMMON_CHARSET_CHARSET_H_
