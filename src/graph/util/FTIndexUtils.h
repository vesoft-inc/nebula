/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_FT_INDEXUTIL_H_
#define UTIL_FT_INDEXUTIL_H_

#include "common/base/StatusOr.h"
#include "parser/MaintainSentences.h"
#include "util/SchemaUtil.h"
#include "util/ExpressionUtils.h"
#include "common/clients/meta/MetaClient.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"

namespace nebula {
namespace graph {

class FTIndexUtils final {
public:
    FTIndexUtils() = delete;

    static bool needTextSearch(const Expression* expr);

    static
    StatusOr<std::vector<nebula::plugin::HttpClient>>
    getTSClients(meta::MetaClient* client);

    static
    StatusOr<bool>
    checkTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                 const std::string& index);

    static
    StatusOr<bool>
    dropTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                const std::string& index);

    static StatusOr<Expression*> rewriteTSFilter(
        ObjectPool* pool,
        bool isEdge,
        Expression* expr,
        const std::string& index,
        const std::vector<nebula::plugin::HttpClient>& tsClients);

    static
    StatusOr<std::vector<std::string>> textSearch(Expression* expr,
        const std::string& index, const std::vector<nebula::plugin::HttpClient>& tsClients);

    static
    const nebula::plugin::HttpClient& randomFTClient(
        const std::vector<nebula::plugin::HttpClient>& tsClients);
};

}  // namespace graph
}  // namespace nebula
#endif  // UTIL_FT_INDEXUTIL_H_
