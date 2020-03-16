/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CHARSET_UNIUTF8_H_
#define COMMON_CHARSET_UNIUTF8_H_

#include "base/Base.h"
#include "base/StatusOr.h"

namespace nebula {

// Code point structure of each plane in the plane table
struct UniCaseInfo {
    uint16_t  toupper_;
    uint16_t  tolower_;
    uint16_t  sort_;
};


class UniUtf8 final {
public:
    /*
     * Parse a single UTF8 character from a byte string to Unicode
     * uni  a parsed character, using unicode encoding
     * beg  points to the first character of the byte string
     * end  points to the next position of the last byte of byte string
     *
     * retVal
     * Positive: the number of bytes occupied by the parsing character.
     * 0:        the current parsing character is a wrong UTF8 encoding.
     * Negative: the utf8 encoding of current parsing character is incomplete.
     *           and it needs occupy several bytes
     */
    static int utf8ToUni(uint64_t* uni, const unsigned char* beg, const unsigned char* end);

    // Unicode to sort code point in plane table
    static void toSortUnicode(uint64_t* uni);

    // Binary comparison
    static int utf8BinCmp(const unsigned char* src,
                          const unsigned char* srcend,
                          const unsigned char* tar,
                          const unsigned char* tarend);

    // Compare utf8 encoding character by character
    static int utf8GeneralCICmp(const std::string& p1, const std::string& p2);

private:
    // Unicode support planes
    static UniCaseInfo   plane00_[];
    static UniCaseInfo   plane01_[];
    static UniCaseInfo   plane02_[];
    static UniCaseInfo   plane03_[];
    static UniCaseInfo   plane04_[];
    static UniCaseInfo   plane05_[];
    static UniCaseInfo   plane1E_[];
    static UniCaseInfo   plane1F_[];
    static UniCaseInfo   plane21_[];
    static UniCaseInfo   plane24_[];
    static UniCaseInfo   planeFF_[];

    // Unicode encoding plane table
    static UniCaseInfo*  uniPlaneTab_[256];

    // The largest character in unicode plane`
    static uint64_t      maxchar_;
};

}   // namespace nebula

#endif  // COMMON_CHARSET_UniUtf8_H_
