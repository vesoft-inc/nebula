/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/FTIndexUtils.h"
#include "common/expression/Expression.h"

DECLARE_uint32(ft_request_retry_times);

namespace nebula {
namespace graph {

bool FTIndexUtils::needTextSearch(const Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kTSFuzzy:
        case Expression::Kind::kTSPrefix:
        case Expression::Kind::kTSRegexp:
        case Expression::Kind::kTSWildcard: {
            return true;
        }
        default:
            return false;
    }
}

StatusOr<std::vector<nebula::plugin::HttpClient>>
FTIndexUtils::getTSClients(meta::MetaClient* client) {
    auto tcs = client->getFTClientsFromCache();
    if (!tcs.ok()) {
        return tcs.status();
    }
    if (tcs.value().empty()) {
        return Status::SemanticError("No text search client found");
    }
    std::vector<nebula::plugin::HttpClient> tsClients;
    for (const auto& c : tcs.value()) {
        nebula::plugin::HttpClient hc;
        hc.host = c.host;
        if (c.user_ref().has_value() && c.pwd_ref().has_value()) {
            hc.user = *c.user_ref();
            hc.password = *c.pwd_ref();
        }
        tsClients.emplace_back(std::move(hc));
    }
    return tsClients;
}

StatusOr<bool>
FTIndexUtils::checkTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                           const std::string& index) {
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        auto ret = nebula::plugin::ESGraphAdapter::kAdapter->indexExists(randomFTClient(tsClients),
                                                                         index);
        if (!ret.ok()) {
            continue;
        }
        return std::move(ret).value();
    }
    return Status::Error("fulltext index get failed : %s", index.c_str());
}

StatusOr<bool>
FTIndexUtils::dropTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                          const std::string& index) {
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        auto ret = nebula::plugin::ESGraphAdapter::kAdapter->dropIndex(randomFTClient(tsClients),
                                                                       index);
        if (!ret.ok()) {
            continue;
        }
        return std::move(ret).value();
    }
    return Status::Error("drop fulltext index failed : %s", index.c_str());
}

StatusOr<Expression*> FTIndexUtils::rewriteTSFilter(
    ObjectPool* pool,
    bool isEdge,
    Expression* expr,
    const std::string& index,
    const std::vector<nebula::plugin::HttpClient>& tsClients) {
    auto vRet = textSearch(expr, index, tsClients);
    if (!vRet.ok()) {
        return Status::SemanticError("Text search error.");
    }
    if (vRet.value().empty()) {
        return nullptr;
    }

    auto tsArg = static_cast<TextSearchExpression*>(expr)->arg();
    Expression* propExpr;
    if (isEdge) {
        propExpr = EdgePropertyExpression::make(pool, tsArg->from(), tsArg->prop());
    } else {
        propExpr = TagPropertyExpression::make(pool, tsArg->from(), tsArg->prop());
    }
    std::vector<Expression*> rels;
    for (const auto& row : vRet.value()) {
        auto constExpr = ConstantExpression::make(pool, Value(row));
        rels.emplace_back(RelationalExpression::makeEQ(pool, propExpr, constExpr));
    }
    if (rels.size() == 1) {
        return rels.front();
    }
    return ExpressionUtils::pushOrs(pool, rels);
}

StatusOr<std::vector<std::string>>
FTIndexUtils::textSearch(Expression* expr,
                         const std::string& index,
                         const std::vector<nebula::plugin::HttpClient>& tsClients) {
    auto tsExpr = static_cast<TextSearchExpression*>(expr);
    // if (*tsExpr->arg()->from() != from_) {
    //     return Status::SemanticError("Schema name error : %s", tsExpr->arg()->from()->c_str());
    // }
    // auto index = plugin::IndexTraits::indexName(*space_.spaceDesc.space_name_ref(), isEdge_);
    nebula::plugin::DocItem doc(index, tsExpr->arg()->prop(), tsExpr->arg()->val());
    nebula::plugin::LimitItem limit(tsExpr->arg()->timeout(), tsExpr->arg()->limit());
    std::vector<std::string> result;
    // TODO (sky) : External index load balancing
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        StatusOr<bool> ret = Status::Error();
        switch (tsExpr->kind()) {
            case Expression::Kind::kTSFuzzy: {
                folly::dynamic fuzz = folly::dynamic::object();
                if (tsExpr->arg()->fuzziness() < 0) {
                    fuzz = "AUTO";
                } else {
                    fuzz = tsExpr->arg()->fuzziness();
                }
                std::string op = tsExpr->arg()->op().empty() ? "or" : tsExpr->arg()->op();
                ret = nebula::plugin::ESGraphAdapter::kAdapter->fuzzy(
                    randomFTClient(tsClients), doc, limit, fuzz, op, result);
                break;
            }
            case Expression::Kind::kTSPrefix: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->prefix(
                    randomFTClient(tsClients), doc, limit, result);
                break;
            }
            case Expression::Kind::kTSRegexp: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->regexp(
                    randomFTClient(tsClients), doc, limit, result);
                break;
            }
            case Expression::Kind::kTSWildcard: {
                ret = nebula::plugin::ESGraphAdapter::kAdapter->wildcard(
                    randomFTClient(tsClients), doc, limit, result);
                break;
            }
            default:
                return Status::SemanticError("text search expression error");
        }
        if (!ret.ok()) {
            continue;
        }
        if (ret.value()) {
            return result;
        }
        return Status::SemanticError("External index error. "
                                     "please check the status of fulltext cluster");
    }
    return Status::SemanticError("scan external index failed");
}

const nebula::plugin::HttpClient& FTIndexUtils::randomFTClient(
    const std::vector<nebula::plugin::HttpClient>& tsClients) {
    auto i = folly::Random::rand32(tsClients.size() - 1);
    return tsClients[i];
}

}  // namespace graph
}  // namespace nebula
