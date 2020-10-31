/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GROUPBY_VALIDATOR_H_
#define VALIDATOR_GROUPBY_VALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/Query.h"


namespace nebula {
namespace graph {

class GroupByValidator final : public Validator {
public:
    GroupByValidator(Sentence *sentence, QueryContext *context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateGroup(const GroupClause *groupClause);

    Status validateYield(const YieldClause *yieldClause);

private:
    std::vector<YieldColumn*>                         groupCols_;
    std::vector<YieldColumn*>                         yieldCols_;

    // key: alias, value: input name
    std::unordered_map<std::string, YieldColumn*>     aliases_;

    std::vector<std::string>                          outputColumnNames_;

    std::vector<Expression*>                          groupKeys_;
    std::vector<Aggregate::GroupItem>                 groupItems_;
};


}  // namespace graph
}  // namespace nebula
#endif
