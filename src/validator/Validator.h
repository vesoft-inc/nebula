/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "common/base/Base.h"
#include "context/ast/AstContext.h"
#include "context/QueryContext.h"
#include "context/ValidateContext.h"
#include "parser/Sentence.h"
#include "planner/plan/ExecutionPlan.h"
#include "planner/Planner.h"
#include "service/PermissionCheck.h"
#include "visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

class Validator {
public:
    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    // validate will call `spaceChosen` -> `validateImpl` -> `checkPermission` -> `toPlan`
    // in order
    static Status validate(Sentence* sentence, QueryContext* qctx);

    Status validate();

    MUST_USE_RESULT Status appendPlan(PlanNode* tail);

    void setInputVarName(std::string name) {
        inputVarName_ = std::move(name);
    }

    QueryContext* qctx() {
        return qctx_;
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

    void setOutputCols(const ColsDef&& outputCols) {
        outputs_ = std::move(outputCols);
    }

    void setOutputCols(ColsDef& outputCols) {
        outputs_ = outputCols;
    }

    std::vector<std::string> getOutColNames() const;

    ColsDef inputCols() const {
        return inputs_;
    }

    void setInputCols(ColsDef&& inputCols) {
        inputs_ = std::move(inputCols);
    }

    void setInputCols(const ColsDef& inputCols) {
        inputs_ = inputCols;
    }

    ExpressionProps exprProps() const {
        return exprProps_;
    }

    void setExprProps(ExpressionProps&& exprProps) {
        exprProps_ = std::move(exprProps);
    }

    void setExprProps(const ExpressionProps& exprProps) {
        exprProps_ = exprProps;
    }

    const std::set<std::string>&  userDefinedVarNameList() const {
        return userDefinedVarNameList_;
    }

    void setUserDefinedVarNameList(std::set<std::string>&& userDefinedVarNameList) {
        userDefinedVarNameList_ = std::move(userDefinedVarNameList);
    }

    void setUserDefinedVarNameList(const std::set<std::string>& userDefinedVarNameList) {
        userDefinedVarNameList_ = userDefinedVarNameList;
    }

    void setNoSpaceRequired() {
        noSpaceRequired_ = true;
    }

    // Whether require choosen space
    bool noSpaceRequired() const {
        return noSpaceRequired_;
    }

    const Sentence* sentence() const {
        return sentence_;
    }

protected:
    Validator(Sentence* sentence, QueryContext* qctx);

    /**
     * Check if a space is chosen for this sentence.
     */
    virtual bool spaceChosen();

    // Do all permission checking in validator except which need execute
    // TODO(shylock) do all permission which don't need execute in here
    virtual Status checkPermission() {
        return PermissionCheck::permissionCheck(
                qctx_->rctx()->session(), sentence_, qctx_->vctx(), space_.id);
    }

    /**
     * Validate the sentence.
     */
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan();

    virtual AstContext* getAstContext() {
        return nullptr;
    }

    StatusOr<Value::Type> deduceExprType(const Expression* expr) const;

    Status deduceProps(const Expression* expr, ExpressionProps& exprProps);

    bool evaluableExpr(const Expression* expr) const;

    static StatusOr<size_t> checkPropNonexistOrDuplicate(const ColsDef& cols,
                                                         folly::StringPiece prop,
                                                         const std::string& validator);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

    // use for simple Plan only contain one node
    template <typename Node, typename... Args>
    Status genSingleNodePlan(Args... args) {
        auto *doNode = Node::make(qctx_, nullptr, std::forward<Args>(args)...);
        root_ = doNode;
        tail_ = root_;
        return Status::OK();
    }

    // Check the variable or input property reference
    // return the input variable
    StatusOr<std::string> checkRef(const Expression *ref, const Value::Type type);

    // Check the output for duplicate column names
    Status checkDuplicateColName();

    Status invalidLabelIdentifiers(const Expression* expr) const;

    template <typename T>
    std::unique_ptr<T> getContext() const {
        auto ctx = std::make_unique<T>();
        ctx->sentence = sentence_;
        ctx->qctx = qctx_;
        ctx->space = space_;
        return ctx;
    }

protected:
    SpaceInfo                       space_;
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    // The variable name of the input node.
    std::string                     inputVarName_;
    // Admin sentences do not requires a space to be chosen.
    bool                            noSpaceRequired_{false};

    // The input columns and output columns of a sentence.
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    ExpressionProps                 exprProps_;

    // root and tail of a subplan.
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    // user define Variable name list
    std::set<std::string>           userDefinedVarNameList_;
    // vid's Type
    Value::Type                     vidType_;
};

}  // namespace graph
}  // namespace nebula
#endif
