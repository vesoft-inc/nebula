/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VALIDATOR_YIELDVALIDATOR_H_
#define GRAPH_VALIDATOR_YIELDVALIDATOR_H_

#include "common/base/Status.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/GroupByValidator.h"
#include "graph/validator/Validator.h"

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
  Status makeOutputColumn(YieldColumn *column);
  Status makeImplicitGroupByValidator();
  Status validateImplicitGroupBy();
  void genConstantExprValues();

 private:
  YieldColumns *columns_{nullptr};
  std::string constantExprVar_;
  std::string userDefinedVarName_;
  Expression *filterCondition_{nullptr};
  // validate for agg
  std::unique_ptr<GroupByValidator> groupByValidator_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_YIELDVALIDATOR_H_
