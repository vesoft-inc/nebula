/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ASTVALIDATOR_H_
#define VALIDATOR_ASTVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/SequentialSentences.h"
#include "service/ClientSession.h"
#include "context/QueryContext.h"

namespace nebula {

class CharsetInfo;

namespace graph {

class ExecutionPlan;

class ASTValidator final {
public:
    ASTValidator(SequentialSentences* sentences,
                 QueryContext* qctx)
        : sentences_(sentences), qctx_(qctx) {}

    Status validate();

private:
    SequentialSentences*                sentences_{nullptr};
    QueryContext*                       qctx_;
};
}  // namespace graph
}  // namespace nebula
#endif
