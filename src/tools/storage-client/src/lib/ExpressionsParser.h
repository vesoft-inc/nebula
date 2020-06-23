/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_STORAGE_CLIENT_EXPRESSIONSPARSER_H_
#define NEBULA_STORAGE_CLIENT_EXPRESSIONSPARSER_H_

#include <common/filter/Expressions.h>
#include "parser/GQLParser.h"

namespace nebula {
#define PARSER_PREFIX_STR "LOOKUP ON temp WHERE "
class ExpressionsParser {
public:
    static bool encodeExpression(const std::string& str, std::string& filter) {
        if (str.empty()) {
            return true;
        }
        std::string query;
        query.append(PARSER_PREFIX_STR).append(str);
        GQLParser parser;
        auto result = parser.parse(query);
        if (!result.ok()) {
            LOG(ERROR) << "Expression parser failed , status : " << result.status();
            return false;
        }
        auto *sentence_ = result.value()->sentences()[0];
        auto* lookup = dynamic_cast<LookupSentence*>(sentence_);
        auto *clause = lookup->whereClause();
        if (!clause) {
            LOG(ERROR) << "Where clause is required";
            return false;
        }
        if (clause->filter()) {
            filter = Expression::encode(clause->filter());
        }
        return true;
    }
};
}   // namespace nebula
#endif   // NEBULA_STORAGE_CLIENT_EXPRESSIONSPARSER_H_
