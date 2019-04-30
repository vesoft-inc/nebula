/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "base/Base.h"
#include "parser/Expressions.h"

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
        kDefineTag,
        kAlterTag,
        kDefineEdge,
        kAlterEdge,
        kDescribeTag,
        kDescribeEdge,
        kInsertVertex,
        kInsertEdge,
        kShow,
        kDeleteVertex,
        kDeleteEdge,
        kFind,
        kAddHosts,
        kRemoveHosts,
        kCreateSpace,
        kDropSpace,
        kYield,
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
