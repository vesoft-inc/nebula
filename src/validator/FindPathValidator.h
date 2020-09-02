/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_FINDPATHVALIDATOR_H_
#define VALIDATOR_FINDPATHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"

namespace nebula {
namespace graph {


class FindPathValidator final : public TraversalValidator {
public:
    FindPathValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    bool            isShortest_{false};
    Starts          from_;
    Starts          to_;
    Over            over_;
    Steps           steps_;
};
}  // namespace graph
}  // namespace nebula
#endif
