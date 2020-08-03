/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "common/base/Base.h"
#include "planner/ExecutionPlan.h"
#include "parser/Sentence.h"
#include "context/ValidateContext.h"
#include "context/QueryContext.h"

namespace nebula {

class YieldColumns;

namespace graph {

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    MUST_USE_RESULT Status appendPlan(PlanNode* tail);

    Status validate();

    void setInputVarName(std::string name) {
        inputVarName_ = std::move(name);
    }

    void setInputCols(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

    PlanNode* root() const {
        return root_;
    }

    PlanNode* tail() const {
        return tail_;
    }

    ColsDef outputCols() const {
        return outputs_;
    }

    ColsDef inputCols() const {
        return inputs_;
    }

    void setNoSpaceRequired() {
        noSpaceRequired_ = true;
    }

protected:
    Validator(Sentence* sentence, QueryContext* qctx);

    /**
     * Check if a space is chosen for this sentence.
     */
    virtual bool spaceChosen();

    /**
     * Validate the sentence.
     */
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan() = 0;

    std::vector<std::string> deduceColNames(const YieldColumns* cols) const;

    std::string deduceColName(const YieldColumn* col) const;

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status deduceProps(const Expression* expr);

    bool evaluableExpr(const Expression* expr) const;

    static Status checkPropNonexistOrDuplicate(const ColsDef& cols, const std::string& prop);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

    // Check the variable or input property reference
    // return the input variable
    StatusOr<std::string> checkRef(const Expression *ref, const Value::Type type) const;

protected:
    SpaceDescription                space_;
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    // root and tail of a subplan.
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    // The input columns and output columns of a sentence.
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    // The variable name of the input node.
    std::string                     inputVarName_;
    // Admin sentences do not requires a space to be chosen.
    bool                            noSpaceRequired_{false};

    // properties
    std::vector<std::string> inputProps_;
    std::unordered_map<std::string, std::vector<std::string>> varProps_;
    std::unordered_map<TagID, std::vector<std::string>> srcTagProps_;
    std::unordered_map<TagID, std::vector<std::string>> dstTagProps_;
    std::unordered_map<EdgeType, std::vector<std::string>> edgeProps_;
    std::unordered_map<TagID, std::vector<std::string>> tagProps_;
};

}  // namespace graph
}  // namespace nebula
#endif
