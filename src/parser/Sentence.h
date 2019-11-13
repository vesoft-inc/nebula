/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {

class Sentence {
public:
    virtual ~Sentence() {}
    virtual std::string toString() const = 0;

    enum class Kind : uint32_t {
        kUnknown,
        kGo,
        kSet,
        kPipe,
        kUse,
        kMatch,
        kAssignment,
        kCreateTag,
        kAlterTag,
        kCreateEdge,
        kAlterEdge,
        kDescribeTag,
        kDescribeEdge,
        kDropTag,
        kDropEdge,
        kInsertVertex,
        kUpdateVertex,
        kInsertEdge,
        kUpdateEdge,
        kShow,
        kDeleteVertex,
        kDeleteEdge,
        kFind,
        kCreateSpace,
        kDropSpace,
        kDescribeSpace,
        kYield,
        kCreateUser,
        kDropUser,
        kAlterUser,
        kGrant,
        kRevoke,
        kChangePassword,
        kDownload,
        kIngest,
        kOrderBy,
        kConfig,
        kFetchVertices,
        kFetchEdges,
        kBalance,
        kFindPath,
        kLimit,
        KGroupBy,
    };

    Kind kind() const {
        return kind_;
    }

protected:
    Kind                kind_{Kind::kUnknown};
};

inline std::ostream& operator<<(std::ostream &os, Sentence::Kind kind) {
    return os << static_cast<uint32_t>(kind);
}

}   // namespace nebula

#endif  // PARSER_SENTENCE_H_
