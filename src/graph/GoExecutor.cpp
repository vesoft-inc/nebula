/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GoExecutor.h"
#include "graph/SchemaHelper.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "dataman/ResultSchemaProvider.h"


namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

GoExecutor::GoExecutor(Sentence *sentence, ExecutionContext *ectx) : TraverseExecutor(ectx) {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<GoSentence*>(sentence);
}


Status GoExecutor::prepare() {
    return Status::OK();
}


Status GoExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        status = prepareStep();
        if (!status.ok()) {
            break;
        }
        status = prepareFrom();
        if (!status.ok()) {
            break;
        }
        status = prepareOver();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = prepareNeededProps();
        if (!status.ok()) {
            break;
        }
        status = prepareDistinct();
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << "Preparing failed: " << status;
        return status;
    }

    return status;
}


void GoExecutor::execute() {
    FLOG_INFO("Executing Go: %s", sentence_->toString().c_str());
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getGoStats());
        return;
    }

    status = setupStarts();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getGoStats());
        return;
    }
    if (starts_.empty()) {
        onEmptyInputs();
        return;
    }
    if (distinct_) {
        std::unordered_set<VertexID> uniqID;
        for (auto id : starts_) {
            uniqID.emplace(id);
        }
        starts_ = std::vector<VertexID>(uniqID.begin(), uniqID.end());
    }
    stepOut();
}


void GoExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    inputs_ = std::move(result);
}


Status GoExecutor::prepareStep() {
    auto *clause = sentence_->stepClause();
    if (clause != nullptr) {
        steps_ = clause->steps();
        upto_ = clause->isUpto();
    }

    if (isUpto()) {
        return Status::Error("`UPTO' not supported yet");
    }

    if (steps_ != 1) {
        backTracker_ = std::make_unique<VertexBackTracker>();
    }

    return Status::OK();
}


Status GoExecutor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
    do {
        if (clause == nullptr) {
            LOG(FATAL) << "From clause shall never be null";
        }

        if (clause->isRef()) {
            auto *expr = clause->ref();
            if (expr->isInputExpression()) {
                fromType_ = kPipe;
                auto *iexpr = static_cast<InputPropertyExpression*>(expr);
                colname_ = iexpr->prop();
            } else if (expr->isVariableExpression()) {
                fromType_ = kVariable;
                auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
                varname_ = vexpr->alias();
                colname_ = vexpr->prop();
            } else {
                // No way to happen except memory corruption
                LOG(FATAL) << "Unknown kind of expression";
            }

            if (colname_ != nullptr && *colname_ == "*") {
                status = Status::Error("Can not use `*' to reference a vertex id column.");
                break;
            }
            break;
        }

        auto space = ectx()->rctx()->session()->space();
        expCtx_->setSpace(space);
        auto vidList = clause->vidList();
        for (auto *expr : vidList) {
            expr->setContext(expCtx_.get());

            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval();
            if (!value.ok()) {
                status = Status::Error();
                break;
            }
            if (expr->isFunCallExpression()) {
                auto *funcExpr = static_cast<FunctionCallExpression*>(expr);
                if (*(funcExpr->name()) == "near") {
                    auto v = Expression::asString(value.value());
                    std::vector<VertexID> result;
                    folly::split(",", v, result, true);
                    starts_.insert(starts_.end(),
                                   std::make_move_iterator(result.begin()),
                                   std::make_move_iterator(result.end()));
                    continue;
                }
            }
            auto v = value.value();
            if (!Expression::isInt(v)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            starts_.push_back(Expression::asInt(v));
        }
        fromType_ = kInstantExpr;
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status GoExecutor::prepareOverAll() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto edgeAllStatus = ectx()->schemaManager()->getAllEdge(spaceId);

    if (!edgeAllStatus.ok()) {
        return edgeAllStatus.status();
    }

    auto allEdge = edgeAllStatus.value();
    for (auto &e : allEdge) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, e);
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        if (isReversely()) {
            v = -v;
        }

        edgeTypes_.push_back(v);

        if (!expCtx_->addEdge(e, v)) {
            return Status::Error(folly::sformat("edge alias({}) was dup", e));
        }
    }

    return Status::OK();
}

Status GoExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(FATAL) << "Over clause shall never be null";
    }

    isReversely_ = clause->isReversely();

    if (isReversely()) {
        edgeHolder_ = std::make_unique<EdgeHolder>();
    }

    auto edges = clause->edges();
    for (auto e : edges) {
        if (e->isOverAll()) {
            expCtx_->setOverAllEdge();
            return prepareOverAll();
        }

        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *e->edge());
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        if (isReversely()) {
            v = -v;
        }
        edgeTypes_.push_back(v);

        if (e->alias() != nullptr) {
            if (!expCtx_->addEdge(*e->alias(), v)) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->alias()));
            }
        } else {
            if (!expCtx_->addEdge(*e->edge(), v)) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->edge()));
            }
        }
    }

    return status;
}


Status GoExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    return Status::OK();
}


Status GoExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    // this preparation depends on interim result,
    // it can only be called after getting results of the previous executor,
    // but if we can do the semantic analysis before execution,
    // then we can do the preparation before execution
    // TODO: make it possible that this preparation not depends on interim result
    if (clause != nullptr) {
        yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
        auto *varHolder = ectx()->variableHolder();
        auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
        if (!status.ok()) {
            return status;
        }
        for (auto *col : yields_) {
            if (!col->getFunName().empty()) {
                return Status::SyntaxError("Do not support in aggregated query without group by");
            }
        }
    }
    return Status::OK();
}


Status GoExecutor::prepareNeededProps() {
    auto status = Status::OK();
    do {
        if (filter_ != nullptr) {
            filter_->setContext(expCtx_.get());
            status = filter_->prepare();
            if (!status.ok()) {
                break;
            }
        }

        for (auto *col : yields_) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }
        if (!status.ok()) {
            break;
        }

        if (expCtx_->hasVariableProp()) {
            if (fromType_ != kVariable) {
                status = Status::Error("A variable must be referred in FROM "
                                       "before used in WHERE or YIELD");
                break;
            }
            auto &variables = expCtx_->variables();
            if (variables.size() > 1) {
                status = Status::Error("Only one variable allowed to use");
                break;
            }
            auto &var = *variables.begin();
            if (var != *varname_) {
                status = Status::Error("Variable name not match: `%s' vs. `%s'",
                                       var.c_str(), varname_->c_str());
                break;
            }
        }

        if (expCtx_->hasInputProp()) {
            if (fromType_ != kPipe) {
                status = Status::Error("`$-' must be referred in FROM "
                                       "before used in WHERE or YIELD");
                break;
            }
        }

        auto &tagMap = expCtx_->getTagMap();
        auto spaceId = ectx()->rctx()->session()->space();
        for (auto &entry : tagMap) {
            auto tagId = ectx()->schemaManager()->toTagID(spaceId, entry.first);
            if (!tagId.ok()) {
                status == Status::Error("Tag `%s' not found.", entry.first.c_str());
                break;
            }
            entry.second = tagId.value();
        }
    } while (false);

    return status;
}


Status GoExecutor::prepareDistinct() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        distinct_ = clause->isDistinct();
        // TODO Consider distinct pushdown later, depends on filter and some other clause pushdown.
        distinctPushDown_ =
            !((expCtx_->hasSrcTagProp() || expCtx_->hasEdgeProp()) && expCtx_->hasDstTagProp());
    }
    return Status::OK();
}


Status GoExecutor::setupStarts() {
    // Literal vertex ids
    if (!starts_.empty()) {
        return Status::OK();
    }
    const auto *inputs = inputs_.get();
    // Take one column from a variable
    if (varname_ != nullptr) {
        bool existing = false;
        auto *varInputs = ectx()->variableHolder()->get(*varname_, &existing);
        if (varInputs == nullptr && !existing) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
        DCHECK(inputs == nullptr);
        inputs = varInputs;
    }
    // No error happened, but we are having empty inputs
    if (inputs == nullptr || !inputs->hasData()) {
        return Status::OK();
    }

    auto result = inputs->getVIDs(*colname_);
    if (!result.ok()) {
        LOG(ERROR) << "Get vid fail: " << *colname_;
        return std::move(result).status();
    }
    starts_ = std::move(result).value();

    auto indexResult = inputs->buildIndex(*colname_);
    if (!indexResult.ok()) {
        return std::move(indexResult).status();
    }
    index_ = std::move(indexResult).value();
    return Status::OK();
}


void GoExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}


void GoExecutor::stepOut() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getStepOutProps();
    if (!status.ok()) {
        doError(Status::Error("Get step out props failed"),
                ectx()->getGraphStats()->getGoStats());
        return;
    }
    auto returns = status.value();
    auto future  = ectx()->getStorageClient()->getNeighbors(spaceId,
                                                            starts_,
                                                            edgeTypes_,
                                                            "",
                                                            std::move(returns));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get neighbors failed"), ectx()->getGraphStats()->getGoStats());
            return;
        } else if (completeness != 100) {
            // TODO(dutor) We ought to let the user know that the execution was partially
            // performed, even in the case that this happened in the intermediate process.
            // Or, make this case configurable at runtime.
            // For now, we just do some logging and keep going.
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        onStepOutResponse(std::move(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Exeception when handle out-bounds/in-bounds."),
                ectx()->getGraphStats()->getGoStats());
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void GoExecutor::onStepOutResponse(RpcResponse &&rpcResp) {
    if (isFinalStep()) {
        maybeFinishExecution(std::move(rpcResp));
        return;
    } else {
        starts_ = getDstIdsFromResp(rpcResp);
        if (starts_.empty()) {
            onEmptyInputs();
            return;
        }
        curStep_++;
        stepOut();
    }
}


void GoExecutor::maybeFinishExecution(RpcResponse &&rpcResp) {
    auto requireDstProps = expCtx_->hasDstTagProp();
    auto requireEdgeProps = !expCtx_->aliasProps().empty();

    // Non-reversely traversal, no properties required on destination nodes
    // Or, Reversely traversal but no properties on edge and destination nodes required.
    // Note that the `dest` which used in reversely traversal means the `src` in foword edge.
    if ((!requireDstProps && !isReversely()) ||
        (isReversely() && !requireDstProps && !requireEdgeProps &&
         !(expCtx_->isOverAllEdge() && yields_.empty()))) {
        finishExecution(std::move(rpcResp));
        return;
    }

    auto dstids = getDstIdsFromResp(rpcResp);

    // Reaching the dead end
    if (dstids.empty()) {
        onEmptyInputs();
        return;
    }

    // Only properties on destination nodes required
    if (!isReversely() || (requireDstProps && !requireEdgeProps)) {
        fetchVertexProps(std::move(dstids), std::move(rpcResp));
        return;
    }

    // Reversely traversal
    DCHECK(isReversely());

    std::unordered_map<EdgeType, std::vector<storage::cpp2::EdgeKey>> edgeKeysMapping;
    std::unordered_map<EdgeType, std::vector<storage::cpp2::PropDef>> edgePropsMapping;

    // TODO: There would be no need to fetch edges' props here,
    // if we implemnet the feature that keep all the props in the reverse edge.
    for (auto &resp : rpcResp.responses()) {
        auto *vertices = resp.get_vertices();
        if (vertices == nullptr) {
            continue;
        }
        auto *eschema = resp.get_edge_schema();
        if (eschema == nullptr) {
            continue;
        }
        std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> schemas;
        std::transform(eschema->cbegin(), eschema->cend(), std::inserter(schemas, schemas.begin()),
                [] (auto &s) {
            return std::make_pair(s.first, std::make_shared<ResultSchemaProvider>(s.second));
        });

        for (auto &vdata : *vertices) {
            for (auto &edge : vdata.edge_data) {
                RowSetReader rsReader(schemas[edge.type], edge.data);
                auto iter = rsReader.begin();
                while (iter) {
                    VertexID dst;
                    EdgeRanking rank;
                    auto rc = iter->getVid(_DST, dst);
                    if (rc != ResultType::SUCCEEDED) {
                        doError(Status::Error("Get dst error when go reversely."),
                                ectx()->getGraphStats()->getGoStats());
                        return;
                    }
                    rc = iter->getVid(_RANK, rank);
                    if (rc != ResultType::SUCCEEDED) {
                        doError(Status::Error("Get rank error when go reversely."),
                                ectx()->getGraphStats()->getGoStats());
                        return;
                    }
                    auto type = std::abs(edge.type);
                    auto &edgeKeys = edgeKeysMapping[type];
                    edgeKeys.emplace_back();
                    edgeKeys.back().set_src(dst);
                    edgeKeys.back().set_dst(vdata.get_vertex_id());
                    edgeKeys.back().set_ranking(rank);
                    edgeKeys.back().set_edge_type(type);
                    ++iter;
                }
            }
        }
    }

    for (auto &prop : expCtx_->aliasProps()) {
        EdgeType edgeType;
        if (!expCtx_->getEdgeType(prop.first, edgeType)) {
            doError(Status::Error("No schema found for `%s'", prop.first.c_str()),
                    ectx()->getGraphStats()->getGoStats());
            return;
        }

        edgeType = std::abs(edgeType);
        auto &edgeProps = edgePropsMapping[edgeType];
        edgeProps.emplace_back();
        edgeProps.back().owner = storage::cpp2::PropOwner::EDGE;
        edgeProps.back().name = prop.second;
        edgeProps.back().id.set_edge_type(edgeType);
    }

    using EdgePropResponse = storage::StorageRpcResponse<storage::cpp2::EdgePropResponse>;
    std::vector<folly::SemiFuture<EdgePropResponse>> futures;

    auto spaceId = ectx()->rctx()->session()->space();
    auto *runner = ectx()->rctx()->runner();

    for (auto &pair : edgeKeysMapping) {
        auto *storage = ectx()->getStorageClient();
        auto future = storage->getEdgeProps(spaceId,
                                            pair.second,
                                            edgePropsMapping[pair.first]);
        futures.emplace_back(std::move(future));
    }

    auto cb = [this, stepResp = std::move(rpcResp),
               dstids = std::move(dstids)] (auto &&result) mutable {
        for (auto &t : result) {
            if (t.hasException()) {
                LOG(ERROR) << "Exception caught: " << t.exception().what();
                doError(Status::Error("Exeception when get edge props in reversely traversal."),
                        ectx()->getGraphStats()->getGoStats());
                return;
            }
            auto resp = std::move(t).value();
            for (auto &edgePropResp : resp.responses()) {
                auto status = edgeHolder_->add(edgePropResp);
                if (!status.ok()) {
                    LOG(ERROR) << "Error when handle edges: " << status;
                    doError(std::move(status), ectx()->getGraphStats()->getGoStats());
                    return;
                }
            }
        }

        if (expCtx_->hasDstTagProp()) {
            fetchVertexProps(std::move(dstids), std::move(stepResp));
            return;
        }

        finishExecution(std::move(stepResp));
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Exception when handle edges."),
                ectx()->getGraphStats()->getGoStats());
    };

    folly::collectAll(std::move(futures)).via(runner).thenValue(cb).thenError(error);
}

void GoExecutor::onVertexProps(RpcResponse &&rpcResp) {
    UNUSED(rpcResp);
}

std::vector<std::string> GoExecutor::getEdgeNamesFromResp(RpcResponse &rpcResp) const {
    std::vector<std::string> names;
    auto spaceId = ectx()->rctx()->session()->space();
    auto &resp = rpcResp.responses();
    auto *edgeSchema = resp[0].get_edge_schema();
    if (edgeSchema == nullptr) {
        return names;
    }

    for (auto &schema : *edgeSchema) {
        auto edgeType = schema.first;
        auto status = ectx()->schemaManager()->toEdgeName(spaceId, std::abs(edgeType));
        DCHECK(status.ok());
        auto edgeName = status.value();
        names.emplace_back(std::move(edgeName));
    }

    return names;
}

std::vector<VertexID> GoExecutor::getDstIdsFromResp(RpcResponse &rpcResp) const {
    std::unordered_set<VertexID> set;
    for (auto &resp : rpcResp.responses()) {
        auto *vertices = resp.get_vertices();
        if (vertices == nullptr) {
            continue;
        }

        auto *eschema = resp.get_edge_schema();
        if (eschema == nullptr) {
            continue;
        }
        std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> schema;

        std::transform(eschema->cbegin(), eschema->cend(), std::inserter(schema, schema.begin()),
                       [](auto &s) {
                           return std::make_pair(
                               s.first, std::make_shared<ResultSchemaProvider>(s.second));
                       });

        for (auto &vdata : *vertices) {
            for (auto &edata : vdata.edge_data) {
                auto it = schema.find(edata.type);
                DCHECK(it != schema.end());
                RowSetReader rsReader(it->second, edata.data);
                auto iter = rsReader.begin();
                while (iter) {
                    VertexID dst;
                    auto rc = iter->getVid(_DST, dst);
                    CHECK(rc == ResultType::SUCCEEDED);
                    if (!isFinalStep() && backTracker_ != nullptr) {
                        backTracker_->add(vdata.get_vertex_id(), dst);
                    }
                    set.emplace(dst);
                    ++iter;
                }
            }
        }
    }
    return std::vector<VertexID>(set.begin(), set.end());
}

void GoExecutor::finishExecution(RpcResponse &&rpcResp) {
    // MayBe we can do better.
    std::vector<std::unique_ptr<YieldColumn>> yc;
    if (expCtx_->isOverAllEdge() && yields_.empty()) {
        auto edgeNames = getEdgeNamesFromResp(rpcResp);
        if (edgeNames.empty()) {
            doError(Status::Error("get edge name failed"), ectx()->getGraphStats()->getGoStats());
            return;
        }
        for (const auto &name : edgeNames) {
            auto dummy = new std::string(name);
            auto dummy_exp = new EdgeDstIdExpression(dummy);
            auto ptr = std::make_unique<YieldColumn>(dummy_exp);
            dummy_exp->setContext(expCtx_.get());
            yields_.emplace_back(ptr.get());
            yc.emplace_back(std::move(ptr));
        }
    }

    std::unique_ptr<InterimResult> outputs;
    if (!setupInterimResult(std::move(rpcResp), outputs)) {
        return;
    }

    if (onResult_) {
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(getResultColumnNames());
        if (outputs != nullptr && outputs->hasData()) {
            auto ret = outputs->getRows();
            if (!ret.ok()) {
                LOG(ERROR) << "Get rows failed: " << ret.status();
                doError(std::move(ret).status(), ectx()->getGraphStats()->getGoStats());
                return;
            }
            resp_->set_rows(std::move(ret).value());
        }
    }
    doFinish(Executor::ProcessControl::kNext, ectx()->getGraphStats()->getGoStats());
}

StatusOr<std::vector<storage::cpp2::PropDef>> GoExecutor::getStepOutProps() {
    std::vector<storage::cpp2::PropDef> props;
    for (auto &e : edgeTypes_) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = _DST;
        pd.id.set_edge_type(e);
        props.emplace_back(std::move(pd));
        // We need ranking when go reverly in final step,
        // because we have to fetch the coresponding edges.
        if (isReversely() && isFinalStep()) {
            storage::cpp2::PropDef rankPd;
            rankPd.owner = storage::cpp2::PropOwner::EDGE;
            rankPd.name = _RANK;
            rankPd.id.set_edge_type(e);
            props.emplace_back(std::move(rankPd));
        }
    }

    if (!isFinalStep()) {
        return props;
    }

    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &tagProp : expCtx_->srcTagProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = tagProp.second;
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagProp.first.c_str());
        }
        auto tagId = status.value();
        pd.id.set_tag_id(tagId);
        props.emplace_back(std::move(pd));
    }

    if (isReversely()) {
        return props;
    }

    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name  = prop.second;

        EdgeType edgeType;

        if (!expCtx_->getEdgeType(prop.first, edgeType)) {
            return Status::Error("the edge was not found '%s'", prop.first.c_str());
        }
        pd.id.set_edge_type(edgeType);
        props.emplace_back(std::move(pd));
    }

    return props;
}


StatusOr<std::vector<storage::cpp2::PropDef>> GoExecutor::getDstProps() {
    std::vector<storage::cpp2::PropDef> props;
    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &tagProp : expCtx_->dstTagProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::DEST;
        pd.name = tagProp.second;
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagProp.first.c_str());
        }
        auto tagId = status.value();
        pd.id.set_tag_id(tagId);
        props.emplace_back(std::move(pd));
    }
    return props;
}


void GoExecutor::fetchVertexProps(std::vector<VertexID> ids, RpcResponse &&rpcResp) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getDstProps();
    if (!status.ok()) {
        doError(Status::Error("Get dest props failed"), ectx()->getGraphStats()->getGoStats());
        return;
    }
    auto returns = status.value();
    auto future = ectx()->getStorageClient()->getVertexProps(spaceId, ids, returns);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, stepOutResp = std::move(rpcResp)] (auto &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get dest props failed"), ectx()->getGraphStats()->getGoStats());
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (vertexHolder_ == nullptr) {
            vertexHolder_ = std::make_unique<VertexHolder>();
        }
        for (auto &resp : result.responses()) {
            vertexHolder_->add(resp);
        }
        finishExecution(std::move(stepOutResp));
        return;
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Exception when handle vertex props."),
                ectx()->getGraphStats()->getGoStats());
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


std::vector<std::string> GoExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}


bool GoExecutor::setupInterimResult(RpcResponse &&rpcResp, std::unique_ptr<InterimResult> &result) {
    // Generic results
    result = std::make_unique<InterimResult>(getResultColumnNames());
    std::shared_ptr<SchemaWriter> schema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto uniqResult = std::make_unique<std::unordered_set<std::string>>();
    auto cb = [&] (std::vector<VariantType> record,
                       std::vector<nebula::cpp2::SupportedType> colTypes) {
        if (schema == nullptr) {
            schema = std::make_shared<SchemaWriter>();
            auto colnames = getResultColumnNames();
            if (record.size() != colTypes.size()) {
                LOG(FATAL) << "data nums: " << record.size()
                           << " != type nums: " << colTypes.size();
            }
            for (auto i = 0u; i < record.size(); i++) {
                SupportedType type;
                if (colTypes[i] == SupportedType::UNKNOWN) {
                    switch (record[i].which()) {
                        case VAR_INT64:
                            // all integers in InterimResult are regarded as type of INT
                            type = SupportedType::INT;
                            break;
                        case VAR_DOUBLE:
                            type = SupportedType::DOUBLE;
                            break;
                        case VAR_BOOL:
                            type = SupportedType::BOOL;
                            break;
                        case VAR_STR:
                            type = SupportedType::STRING;
                            break;
                        default:
                            LOG(FATAL) << "Unknown VariantType: " << record[i].which();
                    }
                } else {
                    type = colTypes[i];
                }
                schema->appendCol(colnames[i], type);
            }  // for
            rsWriter = std::make_unique<RowSetWriter>(schema);
        }  // if

        RowWriter writer(schema);
        for (auto &column : record) {
            switch (column.which()) {
                case VAR_INT64:
                    writer << boost::get<int64_t>(column);
                    break;
                case VAR_DOUBLE:
                    writer << boost::get<double>(column);
                    break;
                case VAR_BOOL:
                    writer << boost::get<bool>(column);
                    break;
                case VAR_STR:
                    writer << boost::get<std::string>(column);
                    break;
                default:
                    LOG(FATAL) << "Unknown VariantType: " << column.which();
            }
        }
        // TODO Consider float/double, and need to reduce mem copy.
        std::string encode = writer.encode();
        if (distinct_) {
            auto ret = uniqResult->emplace(encode);
            if (ret.second) {
                rsWriter->addRow(std::move(encode));
            }
        } else {
            rsWriter->addRow(std::move(encode));
        }
    };  // cb
    if (!processFinalResult(rpcResp, cb)) {
        return false;
    }

    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return true;
}


void GoExecutor::onEmptyInputs() {
    auto resultColNames = getResultColumnNames();
    auto outputs = std::make_unique<InterimResult>(std::move(resultColNames));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext, ectx()->getGraphStats()->getGoStats());
}


bool GoExecutor::processFinalResult(RpcResponse &rpcResp, Callback cb) const {
    auto all = rpcResp.responses();
    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &resp : all) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }

        std::unordered_map<TagID, std::shared_ptr<ResultSchemaProvider>> tagSchema;
        auto *vschema = resp.get_vertex_schema();
        if (vschema != nullptr) {
            std::transform(vschema->cbegin(), vschema->cend(),
                           std::inserter(tagSchema, tagSchema.begin()), [](auto &schema) {
                               return std::make_pair(
                                   schema.first,
                                   std::make_shared<ResultSchemaProvider>(schema.second));
                           });
        }

        std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> edgeSchema;
        auto *eschema = resp.get_edge_schema();
        if (eschema != nullptr) {
            std::transform(eschema->cbegin(), eschema->cend(),
                           std::inserter(edgeSchema, edgeSchema.begin()), [](auto &schema) {
                               return std::make_pair(
                                   schema.first,
                                   std::make_shared<ResultSchemaProvider>(schema.second));
                           });
        }

        if (tagSchema.empty() && edgeSchema.empty()) {
            continue;
        }

        for (auto &vdata : resp.vertices) {
            DCHECK(vdata.__isset.edge_data);
            auto tagData = vdata.get_tag_data();
            for (auto &edata : vdata.edge_data) {
                auto it = edgeSchema.find(edata.type);
                DCHECK(it != edgeSchema.end());
                RowSetReader rsReader(it->second, edata.data);
                auto iter = rsReader.begin();
                auto edgeType = edata.type;
                while (iter) {
                    std::vector<SupportedType> colTypes;
                    bool saveTypeFlag = false;
                    auto &getters = expCtx_->getters();
                    getters.getAliasProp = [&iter,
                                            &spaceId,
                                            &edgeType,
                                            &saveTypeFlag,
                                            &colTypes,
                                            &edgeSchema,
                                            srcid = vdata.get_vertex_id(),
                                            this](const std::string &edgeName,
                                                  const std::string &prop) -> OptVariantType {
                        EdgeType type;
                        auto found = expCtx_->getEdgeType(edgeName, type);
                        if (!found) {
                            return Status::Error(
                                    "Get edge type for `%s' failed in getters.", edgeName.c_str());
                        }

                        if (isReversely()) {
                            auto dst = RowReader::getPropByName(&*iter, _DST);
                            if (saveTypeFlag) {
                                colTypes.back() = edgeHolder_->getType(
                                                    boost::get<VertexID>(value(dst)),
                                                    srcid,
                                                    std::abs(edgeType), prop);
                            }
                            if (std::abs(edgeType) != std::abs(type)) {
                                return edgeHolder_->getDefaultProp(std::abs(type), prop);
                            }

                            return edgeHolder_->get(boost::get<VertexID>(value(dst)),
                                                    srcid,
                                                    std::abs(edgeType), prop);
                        } else {
                            if (saveTypeFlag) {
                                colTypes.back() = iter->getSchema()->getFieldType(prop).type;
                            }
                            if (std::abs(edgeType) != std::abs(type)) {
                                auto sit = edgeSchema.find(type);
                                if (sit == edgeSchema.end()) {
                                    return Status::Error("get schema failed");
                                }
                                return RowReader::getDefaultProp(sit->second.get(), prop);
                            }
                            auto res = RowReader::getPropByName(&*iter, prop);
                            if (!ok(res)) {
                                return Status::Error(
                                        folly::sformat("get prop({}.{}) failed", edgeName, prop));
                            }

                            return value(std::move(res));
                        }
                    };
                    getters.getSrcTagProp =
                        [&iter, &spaceId, &tagData, &tagSchema, &saveTypeFlag, &colTypes, this](
                            const std::string &tag, const std::string &prop) -> OptVariantType {
                        TagID tagId;
                        auto found = expCtx_->getTagId(tag, tagId);
                        if (!found) {
                            return Status::Error(
                                    "Get tag id for `%s' failed in getters.", tag.c_str());
                        }

                        auto it2 =
                            std::find_if(tagData.cbegin(), tagData.cend(), [&tagId](auto &td) {
                                if (td.tag_id == tagId) {
                                    return true;
                                }

                                return false;
                            });

                        if (it2 == tagData.cend()) {
                            return RowReader::getDefaultProp(iter->getSchema().get(), prop);
                        }

                        if (saveTypeFlag) {
                            colTypes.back() = tagSchema[tagId]->getFieldType(prop).type;
                        }
                        DCHECK(it2->__isset.data);
                        auto vreader = RowReader::getRowReader(it2->data, tagSchema[tagId]);
                        auto res = RowReader::getPropByName(vreader.get(), prop);
                        if (!ok(res)) {
                            return Status::Error(
                                folly::sformat("get prop({}.{}) failed", tag, prop));
                        }
                        return value(res);
                    };
                    getters.getDstTagProp = [&iter, &spaceId, &saveTypeFlag, &colTypes, this](
                                                const std::string &tag,
                                                const std::string &prop) -> OptVariantType {
                        auto dst = RowReader::getPropByName(&*iter, "_dst");
                        if (!ok(dst)) {
                            return Status::Error(
                                folly::sformat("get prop({}.{}) failed", tag, prop));
                        }
                        auto vid = boost::get<int64_t>(value(std::move(dst)));

                        TagID tagId;
                        auto found = expCtx_->getTagId(tag, tagId);
                        if (!found) {
                            return Status::Error(
                                    "Get tag id for `%s' failed in getters.", tag.c_str());
                        }

                        if (saveTypeFlag) {
                            SupportedType type = vertexHolder_->getType(vid, tagId, prop);
                            colTypes.back() = type;
                        }
                        return vertexHolder_->get(vid, tagId, prop);
                    };
                    getters.getVariableProp = [&saveTypeFlag, &colTypes, &vdata,
                                               this](const std::string &prop) {
                        if (saveTypeFlag) {
                            colTypes.back() = getPropTypeFromInterim(prop);
                        }
                        return getPropFromInterim(vdata.get_vertex_id(), prop);
                    };
                    getters.getInputProp = [&saveTypeFlag, &colTypes, &vdata,
                                            this](const std::string &prop) {
                        if (saveTypeFlag) {
                            colTypes.back() = getPropTypeFromInterim(prop);
                        }
                        return getPropFromInterim(vdata.get_vertex_id(), prop);
                    };

                    // Evaluate filter
                    if (filter_ != nullptr) {
                        auto value = filter_->eval();
                        if (!value.ok()) {
                            doError(std::move(value).status(),
                                    ectx()->getGraphStats()->getGoStats());
                            return false;
                        }
                        if (!Expression::asBool(value.value())) {
                            ++iter;
                            continue;
                        }
                    }
                    std::vector<VariantType> record;
                    record.reserve(yields_.size());
                    saveTypeFlag = true;
                    for (auto *column : yields_) {
                        colTypes.emplace_back(SupportedType::UNKNOWN);
                        auto *expr = column->expr();
                        auto value = expr->eval();
                        if (!value.ok()) {
                            doError(std::move(value).status(),
                                    ectx()->getGraphStats()->getGoStats());
                            return false;
                        }
                        if (column->expr()->isTypeCastingExpression()) {
                            auto exprPtr = static_cast<TypeCastingExpression *>(column->expr());
                            colTypes.back() = SchemaHelper::columnTypeToSupportedType(
                                                    exprPtr->getType());
                        }
                        record.emplace_back(std::move(value.value()));
                    }
                    cb(std::move(record), std::move(colTypes));
                    ++iter;
                }  // while `iter'
            }
        }   // for `vdata'
    }   // for `resp'
    return true;
}

OptVariantType GoExecutor::VertexHolder::getDefaultProp(TagID tid, const std::string &prop) const {
    for (auto it = data_.cbegin(); it != data_.cend(); ++it) {
        auto it2 = it->second.find(tid);
        if (it2 != it->second.cend()) {
            return RowReader::getDefaultProp(std::get<0>(it2->second).get(), prop);
        }
    }

    return Status::Error("Unknown Vertex");
}

SupportedType GoExecutor::VertexHolder::getDefaultPropType(TagID tid,
                                                           const std::string &prop) const {
    for (auto it = data_.cbegin(); it != data_.cend(); ++it) {
        auto it2 = it->second.find(tid);
        if (it2 != it->second.cend()) {
            return std::get<0>(it2->second)->getFieldType(prop).type;
        }
    }

    return nebula::cpp2::SupportedType::UNKNOWN;
}

OptVariantType GoExecutor::VertexHolder::get(VertexID id, TagID tid,
                                             const std::string &prop) const {
    auto iter = data_.find(id);
    if (iter == data_.end()) {
        return getDefaultProp(tid, prop);
    }

    auto iter2 = iter->second.find(tid);
    if (iter2 == iter->second.end()) {
        return getDefaultProp(tid, prop);
    }

    auto reader = RowReader::getRowReader(std::get<1>(iter2->second), std::get<0>(iter2->second));

    auto res = RowReader::getPropByName(reader.get(), prop);
    if (!ok(res)) {
        return Status::Error(folly::sformat("get prop({}) failed", prop));
    }
    return value(std::move(res));
}

SupportedType GoExecutor::VertexHolder::getType(VertexID id, TagID tid, const std::string &prop) {
    auto iter = data_.find(id);
    if (iter == data_.end()) {
        return getDefaultPropType(tid, prop);
    }

    auto iter2 = iter->second.find(tid);
    if (iter2 == iter->second.end()) {
        return getDefaultPropType(tid, prop);
    }

    return std::get<0>(iter2->second)->getFieldType(prop).type;
}

void GoExecutor::VertexHolder::add(const storage::cpp2::QueryResponse &resp) {
    auto *vertices = resp.get_vertices();
    if (vertices == nullptr) {
        return;
    }

    auto *vertexSchema = resp.get_vertex_schema();
    if (vertexSchema == nullptr) {
        return;
    }
    for (auto &vdata : *vertices) {
        std::unordered_map<TagID, VData> m;
        for (auto &td : vdata.tag_data) {
            DCHECK(td.__isset.data);
            auto it = vertexSchema->find(td.tag_id);
            DCHECK(it != vertexSchema->end());
            m[td.tag_id] = {std::make_shared<ResultSchemaProvider>(it->second), td.data};
        }
        data_[vdata.vertex_id] = std::move(m);
    }
}


Status GoExecutor::EdgeHolder::add(const storage::cpp2::EdgePropResponse &resp) {
    if (resp.get_schema() == nullptr ||
            resp.get_data() == nullptr ||
            resp.get_data()->empty()) {
        return Status::OK();
    }

    auto eschema = std::make_shared<ResultSchemaProvider>(*resp.get_schema());
    RowSetReader rsReader(eschema, *resp.get_data());
    auto collector = std::make_unique<Collector>();
    auto iter = rsReader.begin();
    while (iter) {
        auto src = collector->getProp(eschema.get(), _SRC, &*iter);
        auto dst = collector->getProp(eschema.get(), _DST, &*iter);
        auto type = collector->getProp(eschema.get(), _TYPE, &*iter);
        if (!src.ok() || !dst.ok() || !type.ok()) {
            ++iter;
            continue;
        }
        auto key = std::make_tuple(boost::get<int64_t>(src.value()),
                                   boost::get<int64_t>(dst.value()),
                                   boost::get<int64_t>(type.value()));
        RowWriter rWriter(eschema);
        auto fields = iter->numFields();
        for (auto i = 0; i < fields; i++) {
            auto result = RowReader::getPropByIndex(&*iter, i);
            if (!ok(result)) {
                return Status::Error("Get prop failed when add edge.");
            }
            collector->collect(value(result), &rWriter);
        }

        VLOG(2) << "EdgeHolder added edge, type: " << type.value() << " src: " << src.value()
                << " dst: " << dst.value();
        edges_.emplace(std::move(key), std::make_pair(eschema, rWriter.encode()));

        schemas_.emplace(boost::get<int64_t>(type.value()), eschema);
        ++iter;
    }

    return Status::OK();
}


OptVariantType GoExecutor::EdgeHolder::get(VertexID src,
                                           VertexID dst,
                                           EdgeType type,
                                           const std::string &prop) const {
    auto iter = edges_.find(std::make_tuple(src, dst, type));
    CHECK(iter != edges_.end());
    auto reader = RowReader::getRowReader(iter->second.second, iter->second.first);
    auto result = RowReader::getPropByName(reader.get(), prop);
    if (!ok(result)) {
        return Status::Error("Prop not found: `%s'", prop.c_str());
    }
    return value(result);
}


SupportedType GoExecutor::EdgeHolder::getType(VertexID src,
                                              VertexID dst,
                                              EdgeType type,
                                              const std::string &prop) const {
    auto iter = edges_.find(std::make_tuple(src, dst, type));
    CHECK(iter != edges_.end());
    return iter->second.first->getFieldType(prop).type;
}


OptVariantType GoExecutor::EdgeHolder::getDefaultProp(EdgeType type,
                                                      const std::string &prop) {
    auto sit = schemas_.find(type);
    if (sit == schemas_.end()) {
        // This means that the reversely edge does not exist
        if (prop == _DST || prop == _SRC || prop == _RANK) {
            return static_cast<int64_t>(0);
        } else {
            return Status::Error("Get default prop failed in reversely traversal.");
        }
    }

    return RowReader::getDefaultProp(sit->second.get(), prop);
}


OptVariantType GoExecutor::getPropFromInterim(VertexID id, const std::string &prop) const {
    auto rootId = id;
    if (backTracker_ != nullptr) {
        DCHECK_NE(steps_ , 1u);
        rootId = backTracker_->get(id);
    }
    DCHECK(index_ != nullptr);
    return index_->getColumnWithVID(rootId, prop);
}


SupportedType GoExecutor::getPropTypeFromInterim(const std::string &prop) const {
    DCHECK(index_ != nullptr);
    return index_->getColumnType(prop);
}

}   // namespace graph
}   // namespace nebula
