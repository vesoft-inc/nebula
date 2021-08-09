/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_TRAVERSALVALIDATOR_H_
#define VALIDATOR_TRAVERSALVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/plan/Query.h"
#include "context/ast/QueryAstContext.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

// some utils for the validator to traverse the graph
class TraversalValidator : public Validator {
protected:
    TraversalValidator(Sentence* sentence, QueryContext* qctx) : Validator(sentence, qctx) {
    }

    Status validateStarts(const VerticesClause* clause, Starts& starts);

    Status validateOver(const OverClause* clause, Over& over);

    Status validateStep(const StepClause* clause, StepClause& step);
};

}  // namespace graph
}  // namespace nebula

#endif
