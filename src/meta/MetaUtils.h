/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "base/Base.h"
#include <fnmatch.h>

namespace nebula {
namespace meta {

/**
 * layer(8) + path
 * */

class MetaUtils final {
public:
    ~MetaUtils() = default;

    /**
     * Check current path is valid or not.
     * The path could only contains [0-9], [a-zA-Z] and "_"
     * */
    static bool pathValid(const std::string& path) {
        if (path.size() > 255) {
            return false;
        }
        static const std::regex pattern("^/[a-zA-Z0-9_/]*$");
        return std::regex_match(path, pattern);
    }

    /**
     * Normalize the path. Currently just remove the last slash except root("/")
     * */
    static std::string normalize(const std::string& path) {
        static const std::regex pattern("[/]{2,}");
        auto result = std::regex_replace(path, pattern, "/");
        if (result.size() > 1 && result[result.size() - 1] == '/') {
            return result.substr(0, result.size() - 1);
        }
        return result;
    }

    /**
     * Construct key stored in kvstore.
     * */
    static std::string metaKey(uint8_t layer, const folly::StringPiece& path) {
        std::string key;
        key.reserve(sizeof(layer) + path.size());
        key.append(reinterpret_cast<const char*>(&layer), sizeof(layer));
        key.append(path.data(), path.size());
        return key;
    }

    /**
     * Return the layer current path belonged to.
     * For example, "/" represent layer 0, "/a/b/c" represent layer 3
     * NOTES: we count slash number, so the path should not end with / except root("/").
     * */
    static uint8_t layer(folly::StringPiece path) {
        if (path.size() == 1) {
            CHECK_EQ("/", path);
            return 0;
        }
        uint8_t slashNum = 1;
        for (uint32_t i = 1; i < static_cast<uint32_t>(path.size()); i++) {
            if (path[i] == '/') {
                slashNum++;
            }
        }
        return slashNum;
    }

private:
    MetaUtils() = delete;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METAUTILS_H_

