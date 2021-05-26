/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_PLUGIN_ESGRAPHADAPTER_H
#define NEBULA_PLUGIN_ESGRAPHADAPTER_H

#include <gtest/gtest.h>
#include "common/plugin/fulltext/FTGraphAdapter.h"

namespace nebula {
namespace plugin {
class ESGraphAdapter final : public FTGraphAdapter {
    FRIEND_TEST(FulltextPluginTest, ESIndexCheckTest);
    FRIEND_TEST(FulltextPluginTest, ESResultTest);
    FRIEND_TEST(FulltextPluginTest, ESPrefixTest);
    FRIEND_TEST(FulltextPluginTest, ESWildcardTest);
    FRIEND_TEST(FulltextPluginTest, ESRegexpTest);
    FRIEND_TEST(FulltextPluginTest, ESFuzzyTest);
    FRIEND_TEST(FulltextPluginTest, ESCreateIndexTest);
    FRIEND_TEST(FulltextPluginTest, ESDropIndexTest);

public:
    static std::unique_ptr<FTGraphAdapter> kAdapter;

    StatusOr<bool> prefix(const HttpClient& client,
                          const DocItem& item,
                          const LimitItem& limit,
                          std::vector<std::string>& rows) const override;

    StatusOr<bool> wildcard(const HttpClient& client,
                            const DocItem& item,
                            const LimitItem& limit,
                            std::vector<std::string>& rows) const override;

    StatusOr<bool> regexp(const HttpClient& client,
                          const DocItem& item,
                          const LimitItem& limit,
                          std::vector<std::string>& rows) const override;

    StatusOr<bool> fuzzy(const HttpClient& client,
                         const DocItem& item,
                         const LimitItem& limit,
                         const folly::dynamic& fuzziness,
                         const std::string& op,
                         std::vector<std::string>& rows) const override;

    StatusOr<bool> createIndex(const HttpClient& client,
                               const std::string& index,
                               const std::string& indexTemplate = "") const override;

    StatusOr<bool> dropIndex(const HttpClient& client,
                             const std::string& index) const override;

    StatusOr<bool> indexExists(const HttpClient& client,
                               const std::string& index) const override;

private:
    ESGraphAdapter() {}

    std::string header() const noexcept;

    std::string header(const HttpClient& client,
                       const DocItem& item,
                       const LimitItem& limit) const noexcept;

    folly::dynamic columnBody(const std::string& col) const noexcept;

    std::string body(const DocItem& item,
                     int32_t maxRows,
                     FT_SEARCH_OP type,
                     const folly::dynamic& fuzziness = nullptr,
                     const std::string& op = "") const noexcept;

    folly::dynamic prefixBody(const std::string& prefix) const noexcept;

    folly::dynamic wildcardBody(const std::string& wildcard) const noexcept;

    folly::dynamic regexpBody(const std::string& regexp) const noexcept;

    folly::dynamic fuzzyBody(const std::string& regexp,
                             const folly::dynamic& fuzziness,
                             const std::string& op) const noexcept;

    bool result(const std::string& ret, std::vector<std::string>& rows) const;

    bool statusCheck(const std::string& ret) const;

    bool indexCheck(const std::string& ret) const;

    std::string createIndexCmd(const HttpClient& client,
                               const std::string& index,
                               const std::string& indexTemplate = "") const noexcept;

    std::string dropIndexCmd(const HttpClient& client,
                             const std::string& index) const noexcept;

    std::string indexExistsCmd(const HttpClient& client,
                               const std::string& index) const noexcept;
};
}  // namespace plugin
}  // namespace nebula

#endif   // NEBULA_PLUGIN_ESGRAPHADAPTER_H
