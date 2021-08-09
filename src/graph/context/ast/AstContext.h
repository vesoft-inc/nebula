/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_ASTCONTEXT_H_
#define CONTEXT_ASTCONTEXT_H_

#include "context/QueryContext.h"
#include "parser/Sentence.h"

namespace nebula {
namespace graph {

struct AstContext {
    QueryContext*   qctx;
    Sentence*       sentence;
    SpaceInfo       space;
};

}  // namespace graph
}  // namespace nebula

#endif  // CONTEXT_ASTCONTEXT_H_
