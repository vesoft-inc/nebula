/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_ASTUTIL_H_
#define UTIL_ASTUTIL_H_

#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

class AstUtils final {
public:
    explicit AstUtils(...) = delete;

    static Status reprAstCheck(const Sentence& origin, QueryContext* qctx) {
        auto toString = origin.toString();
        auto copyResult = GQLParser(qctx).parse(toString);
        if (!copyResult.ok()) {
            return Status::Error("The repr sentence `%s' can't be parsed, error: `%s'.",
                                 toString.c_str(),
                                 copyResult.status().toString().c_str());
        }
        auto copyToString = copyResult.value()->toString();
        if (toString != copyToString) {
            return Status::Error("The reparsed ast `%s' is different from origin `%s'.",
                                 copyToString.c_str(),
                                 toString.c_str());
        }
        return Status::OK();
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // UTIL_ASTUTIL_H_
