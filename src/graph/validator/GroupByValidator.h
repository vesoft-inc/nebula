/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_GROUPBY_VALIDATOR_H_
#define GRAPH_VALIDATOR_GROUPBY_VALIDATOR_H_

#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

class GroupByValidator final : public Validator {
 public:
  GroupByValidator(Sentence *sentence, QueryContext *context) : Validator(sentence, context) {}

  Status validateImpl() override;

  Status toPlan() override;

 private:
  Status validateGroup(const GroupClause *groupClause);

  Status validateYield(const YieldClause *yieldClause);

  Status groupClauseSemanticCheck();

 private:
  std::vector<Expression *> groupKeys_;
  std::vector<Expression *> groupItems_;

  std::vector<std::string> aggOutputColNames_;
  bool needGenProject_{false};
  // used to generate Project node when there is an internally nested
  // aggregateExpression
  YieldColumns *projCols_;
  // just for groupClauseSemanticCheck
  std::vector<Expression *> yieldCols_;
};

}  // namespace graph
}  // namespace nebula
#endif
