/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"
#include "visitor/RewriteMatchLabelVisitor.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

Status MatchValidator::toPlan() {
    switch (entry_) {
        case QueryEntry::kId:
            NG_RETURN_IF_ERROR(buildQueryById());
            NG_RETURN_IF_ERROR(buildProjectVertices());
            break;
        case QueryEntry::kIndex:
            NG_RETURN_IF_ERROR(buildScanNode());
            if (!edgeInfos_.empty()) {
                NG_RETURN_IF_ERROR(buildSteps());
            }
            NG_RETURN_IF_ERROR(buildGetTailVertices());
            if (!edgeInfos_.empty()) {
                NG_RETURN_IF_ERROR(buildTailJoin());
            }
            NG_RETURN_IF_ERROR(buildFilter());
            break;
    }
    NG_RETURN_IF_ERROR(buildReturn());

    return Status::OK();
}


Status MatchValidator::validateImpl() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto &clauses = sentence->clauses();

    if (clauses.size() > 1) {
        return Status::SemanticError("Multi clause MATCH not supported");
    }

    if (!clauses[0]->isMatch()) {
        return Status::SemanticError("First clause must be a MATCH");
    }

    auto *matchClause = static_cast<MatchClause*>(clauses[0].get());

    if (matchClause->isOptional()) {
        return Status::SemanticError("OPTIONAL MATCH not supported");
    }

    NG_RETURN_IF_ERROR(validatePath(matchClause->path()));
    if (matchClause->where() != nullptr) {
        NG_RETURN_IF_ERROR(validateFilter(matchClause->where()->filter()));
    }
    NG_RETURN_IF_ERROR(validateReturn(sentence->ret()));
    return analyzeStartPoint();
}


Status MatchValidator::validatePath(const MatchPath *path) {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();

    nodeInfos_.resize(steps + 1);
    edgeInfos_.resize(steps);
    for (auto i = 0u; i <= steps; i++) {
        auto *node = path->node(i);
        auto *label = node->label();
        auto *alias = node->alias();
        auto *props = node->props();
        auto anonymous = false;
        if (label != nullptr) {
            auto tid = sm->toTagID(space_.id, *label);
            if (!tid.ok()) {
                return Status::Error("`%s': Unknown tag", label->c_str());
            }
            nodeInfos_[i].tid = tid.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(anon_->getVar()));
        }
        if (!aliases_.emplace(*alias, kNode).second) {
            return Status::Error("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(*alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        }
        nodeInfos_[i].anonymous = anonymous;
        nodeInfos_[i].label = label;
        nodeInfos_[i].alias = alias;
        nodeInfos_[i].props = props;
        nodeInfos_[i].filter = filter;
    }

    for (auto i = 0u; i < steps; i++) {
        auto *edge = path->edge(i);
        auto &types = edge->types();
        auto *alias = edge->alias();
        auto *props = edge->props();
        auto direction = edge->direction();
        auto anonymous = false;
        if (!types.empty()) {
            for (auto &type : types) {
                auto etype = sm->toEdgeType(space_.id, *type);
                if (!etype.ok()) {
                    return Status::SemanticError("`%s': Unknown edge type", type->c_str());
                }
                edgeInfos_[i].edgeTypes.emplace_back(etype.value());
                edgeInfos_[i].types.emplace_back(*type);
            }
        }
        auto *stepRange = edge->range();
        if (stepRange != nullptr) {
            if (stepRange->min() != stepRange->max() ||
                    stepRange->min() != 1) {
                return Status::SemanticError("Variable steps not supported");
            }
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(anon_->getVar()));
        }
        if (!aliases_.emplace(*alias, kEdge).second) {
            return Status::SemanticError("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(*alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        }
        edgeInfos_[i].anonymous = anonymous;
        edgeInfos_[i].direction = direction;
        edgeInfos_[i].alias = alias;
        edgeInfos_[i].props = props;
        edgeInfos_[i].filter = filter;
    }

    return Status::OK();
}


Status MatchValidator::validateFilter(const Expression *filter) {
    filter_ = ExpressionUtils::foldConstantExpr(filter);
    NG_RETURN_IF_ERROR(validateAliases({filter_.get()}));

    return Status::OK();
}


Status MatchValidator::validateReturn(MatchReturn *ret) {
    // `RETURN *': return all named nodes or edges
    if (ret->isDistinct()) {
        return Status::SemanticError("DISTINCT not supported");
    }
    if (ret->orderFactors() != nullptr) {
        return Status::SemanticError("ORDER BY not supported");
    }
    if (ret->skip() != nullptr) {
        return Status::SemanticError("SKIP not supported");
    }
    if (ret->limit() != nullptr) {
        return Status::SemanticError("LIMIT not supported");
    }
    if (ret->isAll()) {
        auto makeColumn = [] (const std::string &name) {
            auto *expr = new LabelExpression(name);
            auto *alias = new std::string(name);
            return new YieldColumn(expr, alias);
        };

        auto columns = new YieldColumns();
        auto steps = edgeInfos_.size();

        if (!nodeInfos_[0].anonymous) {
            columns->addColumn(makeColumn(*nodeInfos_[0].alias));
        }

        for (auto i = 0u; i < steps; i++) {
            if (!edgeInfos_[i].anonymous) {
                columns->addColumn(makeColumn(*edgeInfos_[i].alias));
            }
            if (!nodeInfos_[i+1].anonymous) {
                columns->addColumn(makeColumn(*nodeInfos_[i+1].alias));
            }
        }

        if (columns->empty()) {
            return Status::SemanticError("`RETURN *' not allowed if there is no alias");
        }

        ret->setColumns(columns);
    }

    // Check all referencing expressions are valid
    std::vector<const Expression*> exprs;
    exprs.reserve(ret->columns()->size());
    for (auto *col : ret->columns()->columns()) {
        exprs.push_back(col->expr());
    }
    NG_RETURN_IF_ERROR(validateAliases(exprs));

    return Status::OK();
}


Status MatchValidator::validateAliases(const std::vector<const Expression*> &exprs) const {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kLabel,
        Expression::Kind::kLabelAttribute
    };

    for (auto *expr : exprs) {
        auto refExprs = ExpressionUtils::collectAll(expr, kinds);
        if (refExprs.empty()) {
            continue;
        }
        for (auto *refExpr : refExprs) {
            auto kind = refExpr->kind();
            const std::string *name = nullptr;
            if (kind == Expression::Kind::kLabel) {
                name = static_cast<const LabelExpression*>(refExpr)->name();
            } else {
                DCHECK(kind == Expression::Kind::kLabelAttribute);
                name = static_cast<const LabelAttributeExpression*>(refExpr)->left()->name();
            }
            DCHECK(name != nullptr);
            if (aliases_.count(*name) != 1) {
                return Status::SemanticError("Alias used but not defined: `%s'", name->c_str());
            }
        }
    }
    return Status::OK();
}


Status MatchValidator::analyzeStartPoint() {
    // TODO(dutor) Originate from either node or edge at any position
    startFromNode_ = true;
    startIndex_ = 0;
    startExpr_ = nullptr;

    auto &head = nodeInfos_[0];

    if (head.label == nullptr) {
        if (filter_ == nullptr) {
            return Status::SemanticError("Query nodes without limit is not supported.");
        } else {
            if (!edgeInfos_.empty()) {
                return Status::SemanticError("Query by id not support extension.");
            }
            // query from id instead of index
            entry_ = QueryEntry::kId;
            return Status::OK();
        }
    }

    Expression *filter = nullptr;
    if (filter_ != nullptr) {
        filter = makeIndexFilter(*head.label, *head.alias, filter_.get());
    }
    if (filter == nullptr) {
        if (head.props != nullptr && !head.props->items().empty()) {
            filter = makeIndexFilter(*head.label, head.props);
        }
    }
    if (filter == nullptr) {
        return Status::SemanticError("Index cannot be deduced in props or filter");
    }


    scanInfo_.filter = filter;
    scanInfo_.schemaId = head.tid;

    entry_ = QueryEntry::kIndex;  // query from index
    return Status::OK();
}


Expression*
MatchValidator::makeIndexFilter(const std::string &label, const MapExpression *map) const {
    auto &items = map->items();
    Expression *root = new RelationalExpression(Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(label),
                new std::string(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
                new TagPropertyExpression(
                    new std::string(label),
                    new std::string(*items[i].first)),
                items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }
    saveObject(root);

    return root;
}

Expression*
MatchValidator::makeIndexFilter(const std::string &label,
                                const std::string &alias,
                                const Expression *filter) const {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE
    };

    std::vector<const Expression*> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        ands = ExpressionUtils::pullAnds(filter);
    } else {
        return nullptr;
    }

    std::vector<Expression*> relationals;
    for (auto *item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto *binary = static_cast<const BinaryExpression*>(item);
        auto *left = binary->left();
        auto *right = binary->right();
        const LabelAttributeExpression *la = nullptr;
        const ConstantExpression *constant = nullptr;
        if (left->kind() == Expression::Kind::kLabelAttribute &&
                right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(left);
            constant = static_cast<const ConstantExpression*>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(right);
            constant = static_cast<const ConstantExpression*>(left);
        } else {
            continue;
        }

        if (*la->left()->name() != alias) {
            continue;
        }

        auto *tpExpr = new TagPropertyExpression(
                new std::string(label),
                new std::string(*la->right()->name()));
        auto *newConstant = constant->clone().release();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto *rel = new RelationalExpression(item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto *rel = new RelationalExpression(item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto *root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto *left = root;
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, relationals[i]);
    }

    saveObject(root);
    return root;
}


StatusOr<Expression*>
MatchValidator::makeSubFilter(const std::string &alias,
                              const MapExpression *map) const {
    DCHECK(map != nullptr);
    auto &items = map->items();
    DCHECK(!items.empty());
    Expression *root = nullptr;

    // TODO(dutor) Check if evaluable and evaluate
    if (items[0].second->kind() != Expression::Kind::kConstant) {
        return Status::SemanticError("Props must be constant: `%s'",
                items[0].second->toString().c_str());
    }
    root = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        if (items[i].second->kind() != Expression::Kind::kConstant) {
            return Status::SemanticError("Props must be constant: `%s'",
                    items[i].second->toString().c_str());
        }
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }

    saveObject(root);

    return root;
}


Status MatchValidator::buildScanNode() {
    if (!startFromNode_) {
        return Status::SemanticError("Scan from edge not supported now");
    }
    if (startIndex_ != 0) {
        return Status::SemanticError("Only support scan from the head node");
    }

    using IQC = nebula::storage::cpp2::IndexQueryContext;
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back();
    contexts->back().set_filter(Expression::encode(*scanInfo_.filter));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(qctx_,
                                nullptr,
                                space_.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                scanInfo_.schemaId);
    tail_ = scan;
    root_ = scan;

    return Status::OK();
}


Status MatchValidator::buildSteps() {
    gnSrcExpr_ = new VariablePropertyExpression(new std::string(),
                                                new std::string(kVid));
    saveObject(gnSrcExpr_);
    NG_RETURN_IF_ERROR(buildStep());
    for (auto i = 1u; i < edgeInfos_.size(); i++) {
        NG_RETURN_IF_ERROR(buildStep());
        NG_RETURN_IF_ERROR(buildStepJoin());
    }

    return Status::OK();
}


Status MatchValidator::buildStep() {
    curStep_++;

    auto &srcNodeInfo = nodeInfos_[curStep_];
    auto &edgeInfo = edgeInfos_[curStep_];
    auto *gn = GetNeighbors::make(qctx_, root_, space_.id);
    gn->setSrc(gnSrcExpr_);
    auto vertexProps = std::make_unique<std::vector<VertexProp>>();
    if (srcNodeInfo.label != nullptr) {
        VertexProp vertexProp;
        vertexProp.set_tag(srcNodeInfo.tid);
        vertexProps->emplace_back(std::move(vertexProp));
    }
    gn->setVertexProps(std::move(vertexProps));
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (!edgeInfo.edgeTypes.empty()) {
        for (auto edgeType : edgeInfo.edgeTypes) {
            if (edgeInfo.direction == Direction::IN_EDGE) {
                edgeType = -edgeType;
            } else if (edgeInfo.direction == Direction::BOTH) {
                EdgeProp edgeProp;
                edgeProp.set_type(-edgeType);
                edgeProps->emplace_back(std::move(edgeProp));
            }
            EdgeProp edgeProp;
            edgeProp.set_type(edgeType);
            edgeProps->emplace_back(std::move(edgeProp));
        }
    }
    gn->setEdgeProps(std::move(edgeProps));
    gn->setEdgeDirection(edgeInfo.direction);

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    yields->addColumn(new YieldColumn(new EdgeExpression()));
    auto *project = Project::make(qctx_, gn, yields);
    project->setInputVar(gn->outputVar());
    project->setColNames({*srcNodeInfo.alias, *edgeInfo.alias});

    root_ = project;

    auto rewriter = [this] (const Expression *expr) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
        return rewrite(static_cast<const LabelAttributeExpression*>(expr));
    };

    if (srcNodeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        srcNodeInfo.filter->accept(&visitor);
        auto *node = Filter::make(qctx_, root_, srcNodeInfo.filter);
        node->setInputVar(root_->outputVar());
        node->setColNames(root_->colNames());
        root_ = node;
    }

    if (edgeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        edgeInfo.filter->accept(&visitor);
        auto *node = Filter::make(qctx_, root_, edgeInfo.filter);
        node->setInputVar(root_->outputVar());
        node->setColNames(root_->colNames());
        root_ = node;
    }

    gnSrcExpr_ = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(project->outputVar()),
                new std::string(*edgeInfo.alias)),
            new LabelExpression("_dst"));
    saveObject(gnSrcExpr_);

    prevStepRoot_ = thisStepRoot_;
    thisStepRoot_ = root_;

    return Status::OK();
}


Status MatchValidator::buildGetTailVertices() {
    Expression *src = nullptr;
    if (!edgeInfos_.empty()) {
        src = new AttributeExpression(
                new VariablePropertyExpression(
                    new std::string(),
                    new std::string(*edgeInfos_[curStep_].alias)),
                new LabelExpression("_dst"));
    } else {
        src = new VariablePropertyExpression(new std::string(),
                new std::string(kVid));
    }
    saveObject(src);

    auto &nodeInfo = nodeInfos_[curStep_ + 1];
    std::vector<VertexProp> props;
    if (nodeInfo.label != nullptr) {
        VertexProp prop;
        prop.set_tag(nodeInfo.tid);
        props.emplace_back(prop);
    }

    auto *gv = GetVertices::make(qctx_, root_, space_.id, src, std::move(props), {}, true);
    if (thisStepRoot_ != nullptr) {
        gv->setInputVar(thisStepRoot_->outputVar());
    }

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(qctx_, gv, yields);
    project->setInputVar(gv->outputVar());
    project->setColNames({*nodeInfo.alias});
    root_ = project;

    if (nodeInfo.filter != nullptr) {
        auto newFilter = nodeInfo.filter->clone();
        RewriteMatchLabelVisitor visitor([this](auto *expr) {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                return rewrite(static_cast<const LabelAttributeExpression*>(expr));
            });
        newFilter->accept(&visitor);
        auto *filter = Filter::make(qctx_, root_, saveObject(newFilter.release()));
        filter->setInputVar(root_->outputVar());
        filter->setColNames(root_->colNames());
        root_ = filter;
    }

    auto *dedup = Dedup::make(qctx_, root_);
    dedup->setInputVar(root_->outputVar());
    dedup->setColNames(root_->colNames());
    root_ = dedup;

    return Status::OK();
}


Status MatchValidator::buildStepJoin() {
    auto prevStep = curStep_ - 1;
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(prevStepRoot_->outputVar()),
                new std::string(*edgeInfos_[prevStep].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*nodeInfos_[curStep_].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(qctx_,
                                root_,
                                {prevStepRoot_->outputVar(), 0},
                                {thisStepRoot_->outputVar(), 0},
                                {key},
                                {probe});
    auto leftColNames = prevStepRoot_->colNames();
    auto rightColNames = thisStepRoot_->colNames();
    std::vector<std::string> colNames;
    colNames.reserve(leftColNames.size() + rightColNames.size());
    for (auto &name : leftColNames) {
        colNames.emplace_back(std::move(name));
    }
    for (auto &name : rightColNames) {
        colNames.emplace_back(std::move(name));
    }
    join->setColNames(std::move(colNames));
    root_ = join;
    thisStepRoot_ = root_;

    return Status::OK();
}


Status MatchValidator::buildTailJoin() {
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*edgeInfos_[curStep_].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(root_->outputVar()),
                new std::string(*nodeInfos_[curStep_ + 1].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(qctx_,
                                root_,
                                {thisStepRoot_->outputVar(), 0},
                                {root_->outputVar(), 0},
                                {key},
                                {probe});
    auto colNames = thisStepRoot_->colNames();
    colNames.emplace_back(*nodeInfos_[curStep_ + 1].alias);
    join->setColNames(std::move(colNames));
    root_ = join;

    return Status::OK();
}


Status MatchValidator::buildFilter() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto *clause = static_cast<MatchClause*>(sentence->clauses()[0].get())->where();
    if (clause == nullptr) {
        return Status::OK();
    }
    auto *filter = clause->filter();
    auto kind = filter->kind();
    // TODO(dutor) Find a better way to identify where an expr is a boolean one
    if (kind == Expression::Kind::kLabel ||
            kind == Expression::Kind::kLabelAttribute) {
        return Status::SemanticError("Filter should be a boolean expression");
    }

    auto newFilter = filter->clone();
    auto rewriter = [this] (const Expression *expr) {
        if (expr->kind() == Expression::Kind::kLabel) {
            return rewrite(static_cast<const LabelExpression*>(expr));
        } else {
            return rewrite(static_cast<const LabelAttributeExpression*>(expr));
        }
    };
    RewriteMatchLabelVisitor visitor(std::move(rewriter));
    newFilter->accept(&visitor);

    auto *node = Filter::make(qctx_, root_, saveObject(newFilter.release()));
    node->setInputVar(root_->outputVar());
    node->setColNames(root_->colNames());

    root_ = node;

    return Status::OK();
}


Status MatchValidator::buildReturn() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;

    for (auto *col : sentence->ret()->columns()->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn *newColumn = nullptr;
        if (kind == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(label));
        } else if (kind == Expression::Kind::kLabelAttribute) {
            auto *la = static_cast<const LabelAttributeExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(la));
        } else {
            auto newExpr = col->expr()->clone();
            auto rewriter = [this] (const Expression *expr) {
                if (expr->kind() == Expression::Kind::kLabel) {
                    return rewrite(static_cast<const LabelExpression*>(expr));
                } else {
                    return rewrite(static_cast<const LabelAttributeExpression*>(expr));
                }
            };
            RewriteMatchLabelVisitor visitor(std::move(rewriter));
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto *project = Project::make(qctx_, root_, yields);
    project->setInputVar(root_->outputVar());
    project->setColNames(std::move(colNames));
    root_ = project;

    return Status::OK();
}


Expression* MatchValidator::rewrite(const LabelExpression *label) const {
    auto *expr = new VariablePropertyExpression(
            new std::string(),
            new std::string(*label->name()));
    return expr;
}


Expression *MatchValidator::rewrite(const LabelAttributeExpression *la) const {
    auto *expr = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(),
                new std::string(*la->left()->name())),
            new LabelExpression(*la->right()->name()));
    return expr;
}

Status MatchValidator::buildQueryById() {
    auto vidsResult = extractVids(filter_.get());
    NG_RETURN_IF_ERROR(vidsResult);
    auto *ge = GetVertices::make(qctx_, nullptr, space_.id, vidsResult.value().second, {}, {});
    ge->setInputVar(vidsResult.value().first);
    root_ = ge;
    tail_ = ge;
    return Status::OK();
}

Status MatchValidator::buildProjectVertices() {
    auto &srcNodeInfo = nodeInfos_.front();
    auto *columns = qctx_->objPool()->makeAndAdd<YieldColumns>();
    columns->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(qctx_, root_, columns);
    project->setInputVar(root_->outputVar());
    project->setColNames({*srcNodeInfo.alias});
    root_ = project;
    return Status::OK();
}

StatusOr<std::pair<std::string, Expression*>>
MatchValidator::extractVids(const Expression *filter) const {
    QueryExpressionContext dummy;
    if (filter->kind() == Expression::Kind::kRelIn) {
        const auto *inExpr = static_cast<const RelationalExpression*>(filter);
        if (inExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            inExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::SemanticError("Not supported expression.");
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression*>(inExpr->left());
        if (*fCallExpr->name() != "id") {
            return Status::SemanticError("Require id limit.");
        }
        auto *constExpr = const_cast<Expression*>(inExpr->right());
        return listToAnnoVarVid(constExpr->eval(dummy).getList());
    } else if (filter->kind() == Expression::Kind::kRelEQ) {
        const auto *eqExpr = static_cast<const RelationalExpression*>(filter);
        if (eqExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            eqExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::SemanticError("Not supported expression.");
        }
        const auto *fCallExpr = static_cast<const FunctionCallExpression*>(eqExpr->left());
        if (*fCallExpr->name() != "id") {
            return Status::SemanticError("Require id limit.");
        }
        auto *constExpr = const_cast<Expression*>(eqExpr->right());
        return constToAnnoVarVid(constExpr->eval(dummy));
    } else {
        return Status::SemanticError("Not supported expression.");
    }
}

std::pair<std::string, Expression*> MatchValidator::listToAnnoVarVid(const List &list) const {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto &v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                         new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

std::pair<std::string, Expression*> MatchValidator::constToAnnoVarVid(const Value &v) const {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = qctx_->objPool()->makeAndAdd<VariablePropertyExpression>(new std::string(input),
                                                                         new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

}   // namespace graph
}   // namespace nebula
