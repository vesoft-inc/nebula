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

    /**
     * Get the number of characters in UTF8 charset
     */
    size_t getUtf8Charlength(const std::string& str);

    /**
     * Compare strings according to the collation of the specified locale
     */
    StatusOr<int> nebulaStrCmp(const std::string& collateName,
                               const std::string& p1,
                               const std::string& p2);

    /**
     * Compare if string p1 is less than string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpLT(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

    /**
     * Compare if string p1 is less than or equal to string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpLE(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

    /**
     * Compare if string p1 is greater than string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpGT(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

    /**
     * Compare if string p1 is greater than or equal to string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpGE(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

    /**
     * Compare if string p1 is equal to string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpEQ(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

    /**
     * Compare if string p1 is not equal to string p2
     * according to the collation of the specified locale
     */
    StatusOr<bool> nebulaStrCmpNE(const std::string& collateName,
                                  const std::string& p1,
                                  const std::string& p2);

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

    /**
     * Mapping collation to locale
     */
    std::unordered_map<std::string, std::string> collationToLocale_ = {
        {"utf8_bin", "en_US.UTF-8"}
    };
};

}   // namespace nebula

#endif  // COMMON_CHARSET_CHARSET_H_
