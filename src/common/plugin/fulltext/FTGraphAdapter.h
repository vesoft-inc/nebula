/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_G_ADAPTER_H_
#define COMMON_PLUGIN_FULLTEXT_G_ADAPTER_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/plugin/fulltext/FTUtils.h"

namespace nebula {
namespace plugin {

class FTGraphAdapter {
public:
    FTGraphAdapter() = default;

    virtual ~FTGraphAdapter() = default;

    virtual StatusOr<bool> prefix(const HttpClient& client,
                                  const DocItem& index,
                                  const LimitItem& limit,
                                  std::vector<std::string>& rows) const = 0;

    virtual StatusOr<bool> wildcard(const HttpClient& client,
                                    const DocItem& index,
                                    const LimitItem& limit,
                                    std::vector<std::string>& rows) const = 0;

    virtual StatusOr<bool> regexp(const HttpClient& client,
                                  const DocItem& index,
                                  const LimitItem& limit,
                                  std::vector<std::string>& rows) const = 0;

    virtual StatusOr<bool> fuzzy(const HttpClient& client,
                                 const DocItem& index,
                                 const LimitItem& limit,
                                 const folly::dynamic& fuzziness,
                                 const std::string& op,
                                 std::vector<std::string>& rows) const = 0;

    virtual StatusOr<bool> createIndex(const HttpClient& client,
                                       const std::string& index,
                                       const std::string& indexTemplate = "") const = 0;

    virtual StatusOr<bool> dropIndex(const HttpClient& client,
                                     const std::string& index) const = 0;

    virtual StatusOr<bool> indexExists(const HttpClient& client,
                                       const std::string& index) const = 0;
};
}  // namespace plugin
}  // namespace nebula

#endif  // COMMON_PLUGIN_FULLTEXT_G_ADAPTER_H_
