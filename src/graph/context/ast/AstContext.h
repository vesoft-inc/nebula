/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ASTCONTEXT_H_
#define GRAPH_CONTEXT_ASTCONTEXT_H_

#include "graph/context/QueryContext.h"
#include "parser/Sentence.h"

namespace nebula {
namespace graph {

struct AstContext {
  QueryContext* qctx;
  Sentence* sentence;
  SpaceInfo space;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ASTCONTEXT_H_
