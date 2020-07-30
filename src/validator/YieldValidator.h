/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_YIELDVALIDATOR_H_
#define VALIDATOR_YIELDVALIDATOR_H_

#include <vector>

#include "common/base/Status.h"
#include "validator/Validator.h"

namespace nebula {

class Sentence;
class YieldClause;
class YieldColumns;
class WhereClause;
class Expression;
class ObjectPool;

namespace graph {

class QueryContext;

class YieldValidator final : public Validator {
public:
    YieldValidator(Sentence *sentence, QueryContext *qctx);

    Status validateImpl() override;

    Status toPlan() override;

private:
    Status validateYieldAndBuildOutputs(const YieldClause *clause);
    Status validateWhere(const WhereClause *clause);
    Status checkVarProps() const;
    Status checkInputProps() const;
    Status checkColumnRefAggFun(const YieldClause *clause) const;
    YieldColumns *getYieldColumns(YieldColumns *yieldColumns,
                                  ObjectPool *objPool,
                                  size_t numColumns);
    void rebuildYield(YieldClause* yield);

    bool hasAggFun_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_YIELDVALIDATOR_H_
