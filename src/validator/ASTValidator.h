/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ASTVALIDATOR_H_
#define VALIDATOR_ASTVALIDATOR_H_

#include "common/base/Base.h"
#include "parser/Sentence.h"
#include "context/QueryContext.h"

namespace nebula {

class Sentence;

namespace graph {

class ExecutionPlan;

class ASTValidator final {
public:
    ASTValidator(Sentence* sentences, QueryContext* qctx)
        : sentences_(DCHECK_NOTNULL(sentences)), qctx_(DCHECK_NOTNULL(qctx)) {}

    Status validate();

private:
    Sentence*                           sentences_{nullptr};
    QueryContext*                       qctx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
