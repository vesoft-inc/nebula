/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LabelAttributeExpression.h"

namespace nebula {

std::string LabelAttributeExpression::toString() const {
    return left()->toString() + "." + right()->toString();
}

}   // namespace nebula
