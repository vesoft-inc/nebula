/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CHARSET_CHARSET_H_
#define COMMON_CHARSET_CHARSET_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"

namespace nebula {


struct CharsetDesc {
    // Charset name
    std::string               charsetName_;
    // Charset default collation
    std::string               defaultColl_;
    // All collations supported by this charset
    std::vector<std::string>  supportColls_;
    // Charset description info
    std::string               desc_;
    // Maximum byte number of a character by this charset
    int32_t                   maxLen_;
};


class CharsetInfo final {
public:
    static CharsetInfo* instance() {
        static std::unique_ptr<CharsetInfo> charsetInfo(new CharsetInfo());
        return charsetInfo.get();
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
     * Check if charset and collation match
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

    /**
     * Get all supported charsets description information
     */
    std::unordered_map<std::string, CharsetDesc> getCharsetDesc() {
        return charsetDesc_;
    }

private:
    CharsetInfo() {
       charsetDesc_["utf8"] = {"utf8", "utf8_bin", {"utf8_bin"}, "UTF-8 Unicode", 4};
    }

    /**
     * List of supported charsets
     */
    std::unordered_set<std::string> supportCharsets_ = {"utf8"};

    /**
     * List of supported collations
     */
    std::unordered_set<std::string> supportCollations_ = {"utf8_bin"};

    /**
     * Description information of supported charsets
     */
    std::unordered_map<std::string, CharsetDesc> charsetDesc_;
};

}   // namespace nebula

#endif  // COMMON_CHARSET_CHARSET_H_
