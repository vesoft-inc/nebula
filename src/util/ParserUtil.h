/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_PARSERUTIL_H_
#define UTIL_PARSERUTIL_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "context/QueryContext.h"
#include "parser/MaintainSentences.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

class ParserUtil final {
public:
    ParserUtil() = delete;

    static bool isLabel(const Expression *expr) {
        return expr->kind() == Expression::Kind::kLabel ||
               expr->kind() == Expression::Kind::kLabelAttribute;
    }

    static void rewriteLC(QueryContext *qctx,
                          ListComprehensionExpression *lc,
                          const std::string &oldVarName) {
        const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(newVarName, Value());
        auto rewriter = [&oldVarName, &newVarName](const Expression *expr) {
            Expression *ret = nullptr;
            if (expr->kind() == Expression::Kind::kLabel) {
                auto *label = static_cast<const LabelExpression *>(expr);
                if (*label->name() == oldVarName) {
                    ret = new VariableExpression(new std::string(newVarName));
                } else {
                    ret = label->clone().release();
                }
            } else {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                auto *la = static_cast<const LabelAttributeExpression *>(expr);
                if (*la->left()->name() == oldVarName) {
                    const auto &value = la->right()->value();
                    ret =
                        new AttributeExpression(new VariableExpression(new std::string(newVarName)),
                                                new ConstantExpression(value));
                } else {
                    ret = la->clone().release();
                }
            }
            return ret;
        };

        RewriteMatchLabelVisitor visitor(rewriter);

        lc->setOriginString(new std::string(lc->makeString()));
        lc->setInnerVar(new std::string(newVarName));
        if (lc->hasFilter()) {
            Expression *filter = lc->filter();
            Expression *newFilter = nullptr;
            if (isLabel(filter)) {
                newFilter = rewriter(filter);
            } else {
                newFilter = filter->clone().release();
                newFilter->accept(&visitor);
            }
            lc->setFilter(newFilter);
        }
        if (lc->hasMapping()) {
            Expression *mapping = lc->mapping();
            Expression *newMapping = nullptr;
            if (isLabel(mapping)) {
                newMapping = rewriter(mapping);
            } else {
                newMapping = mapping->clone().release();
                newMapping->accept(&visitor);
            }
            lc->setMapping(newMapping);
        }
    }

    static void rewritePred(QueryContext *qctx,
                            PredicateExpression *pred,
                            const std::string &oldVarName) {
        const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(newVarName, Value());
        auto rewriter = [&oldVarName, &newVarName](const Expression *expr) {
            Expression *ret = nullptr;
            if (expr->kind() == Expression::Kind::kLabel) {
                auto *label = static_cast<const LabelExpression *>(expr);
                if (*label->name() == oldVarName) {
                    ret = new VariableExpression(new std::string(newVarName));
                } else {
                    ret = label->clone().release();
                }
            } else {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                auto *la = static_cast<const LabelAttributeExpression *>(expr);
                if (*la->left()->name() == oldVarName) {
                    const auto &value = la->right()->value();
                    ret =
                        new AttributeExpression(new VariableExpression(new std::string(newVarName)),
                                                new ConstantExpression(value));
                } else {
                    ret = la->clone().release();
                }
            }
            return ret;
        };

        RewriteMatchLabelVisitor visitor(rewriter);

        pred->setOriginString(new std::string(pred->makeString()));
        pred->setInnerVar(new std::string(newVarName));
        Expression *filter = pred->filter();
        Expression *newFilter = nullptr;
        if (isLabel(filter)) {
            newFilter = rewriter(filter);
        } else {
            newFilter = filter->clone().release();
            newFilter->accept(&visitor);
        }
        pred->setFilter(newFilter);
    }


    static void rewriteReduce(QueryContext *qctx,
                              ReduceExpression *reduce,
                              const std::string &oldAccName,
                              const std::string &oldVarName) {
        const auto &newAccName = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(newAccName, Value());
        const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
        qctx->ectx()->setValue(newVarName, Value());
        auto rewriter = [oldAccName, newAccName, oldVarName, newVarName](
                            const Expression *expr) {
            Expression *ret = nullptr;
            if (expr->kind() == Expression::Kind::kLabel) {
                auto *label = static_cast<const LabelExpression *>(expr);
                if (*label->name() == oldAccName) {
                    ret = new VariableExpression(new std::string(newAccName));
                } else if (*label->name() == oldVarName) {
                    ret = new VariableExpression(new std::string(newVarName));
                } else {
                    ret = label->clone().release();
                }
            } else {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                auto *la = static_cast<const LabelAttributeExpression *>(expr);
                if (*la->left()->name() == oldAccName) {
                    const auto &value = la->right()->value();
                    ret =
                        new AttributeExpression(new VariableExpression(new std::string(newAccName)),
                                                new ConstantExpression(value));
                } else if (*la->left()->name() == oldVarName) {
                    const auto &value = la->right()->value();
                    ret =
                        new AttributeExpression(new VariableExpression(new std::string(newVarName)),
                                                new ConstantExpression(value));
                } else {
                    ret = la->clone().release();
                }
            }
            return ret;
        };

        RewriteMatchLabelVisitor visitor(rewriter);

        reduce->setOriginString(new std::string(reduce->makeString()));
        reduce->setAccumulator(new std::string(newAccName));
        reduce->setInnerVar(new std::string(newVarName));
        Expression *mapping = reduce->mapping();
        Expression *newMapping = nullptr;
        if (isLabel(mapping)) {
            newMapping = rewriter(mapping);
        } else {
            newMapping = mapping->clone().release();
            newMapping->accept(&visitor);
        }
        reduce->setMapping(newMapping);
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // UTIL_PARSERUTIL_H_
