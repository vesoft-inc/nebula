/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "GoValidator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    Status status;
    do {
        status = validateStep();
        if (!status.ok()) {
            break;
        }

        status = validateFrom();
        if (!status.ok()) {
            break;
        }

        status = validateOver();
        if (!status.ok()) {
            break;
        }

        status = validateWhere();
        if (!status.ok()) {
            break;
        }

        status = validateYield();
        if (!status.ok()) {
            break;
        }
    } while (false);

    return status;
}

Status GoValidator::validateStep() {
    return Status::OK();
}

Status GoValidator::validateFrom() {
    return Status::OK();
}

Status GoValidator::validateOver() {
    return Status::OK();
}

Status GoValidator::validateWhere() {
    return Status::OK();
}

Status GoValidator::validateYield() {
    return Status::OK();
}

Status GoValidator::toPlan() {
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
