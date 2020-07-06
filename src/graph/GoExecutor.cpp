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
#include <boost/functional/hash.hpp>


DEFINE_bool(filter_pushdown, true, "If pushdown the filter to storage.");
DEFINE_bool(trace_go, false, "Whether to dump the detail trace log from one go request");

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

GoExecutor::GoExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "go") {
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
        status = checkNeededProps();
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
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (steps_ == 0) {
        // #2100
        // TODO(shylock) unify the steps checking
        onEmptyInputs();
        return;
    }
    // record means the reponse index [1, steps]
    if (recordFrom_ == 0) {
        recordFrom_ = 1;
        CHECK_GE(steps_, recordFrom_);
    }

    status = setupStarts();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    if (index_ == nullptr) {
        index_ = std::make_unique<InterimIndex>();
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


Status GoExecutor::prepareStep() {
    auto *clause = sentence_->stepClause();
    if (clause != nullptr) {
        recordFrom_ = clause->recordFrom();
        steps_ = clause->recordTo();
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
            LOG(ERROR) << "From clause shall never be null";
            return Status::Error("From clause shall never be null");
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
                LOG(ERROR) << "Unknown kind of expression";
                return Status::Error("Unknown kind of expression");
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
        Getters getters;
        for (auto *expr : vidList) {
            expr->setContext(expCtx_.get());

            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval(getters);
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
        auto status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

        if (!expCtx_->addEdge(e, std::abs(v))) {
            return Status::Error(folly::sformat("edge alias({}) was dup", e));
        }
    }

    return Status::OK();
}

Status GoExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(ERROR) << "Over clause shall never be null";
        return Status::Error("Over clause shall never be null");
    }

    direction_ = clause->direction();

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
        status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

        if (e->alias() != nullptr) {
            if (!expCtx_->addEdge(*e->alias(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->alias()));
            }
        } else {
            if (!expCtx_->addEdge(*e->edge(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->edge()));
            }
        }
    }

    return status;
}

Status GoExecutor::addToEdgeTypes(EdgeType type) {
    switch (direction_) {
        case OverClause::Direction::kForward: {
            edgeTypes_.push_back(type);
            break;
        }
        case OverClause::Direction::kBackward: {
            type = -type;
            edgeTypes_.push_back(type);
            break;
        }
        case OverClause::Direction::kBidirect: {
            edgeTypes_.push_back(type);
            type = -type;
            edgeTypes_.push_back(type);
            break;
        }
        default: {
            return Status::Error("Unkown direction type: %ld", static_cast<int64_t>(direction_));
        }
    }
    return Status::OK();
}

Status GoExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    whereWrapper_ = std::make_unique<WhereWrapper>(clause);
    auto status = whereWrapper_->prepare(expCtx_.get());
    return status;
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
                status = Status::Error("Tag `%s' not found.", entry.first.c_str());
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


Status GoExecutor::checkNeededProps() {
    auto space = ectx()->rctx()->session()->space();
    TagID tagId;
    auto srcTagProps = expCtx_->srcTagProps();
    auto dstTagProps = expCtx_->dstTagProps();
    srcTagProps.insert(srcTagProps.begin(),
                       std::make_move_iterator(dstTagProps.begin()),
                       std::make_move_iterator(dstTagProps.end()));
    for (auto &pair : srcTagProps) {
        auto found = expCtx_->getTagId(pair.first, tagId);
        if (!found) {
            return Status::Error("Tag `%s' not found.", pair.first.c_str());
        }
        auto ts = ectx()->schemaManager()->getTagSchema(space, tagId);
        if (ts == nullptr) {
            return Status::Error("No tag schema for %s", pair.first.c_str());
        }
        if (ts->getFieldIndex(pair.second) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                    pair.second.c_str(), pair.first.c_str());
        }
    }

    EdgeType edgeType;
    auto edgeProps = expCtx_->aliasProps();
    for (auto &pair : edgeProps) {
        auto found = expCtx_->getEdgeType(pair.first, edgeType);
        if (!found) {
            return Status::Error("Edge `%s' not found.", pair.first.c_str());
        }
        auto propName = pair.second;
        if (propName == _SRC || propName == _DST
                || propName == _RANK || propName == _TYPE) {
            continue;
        }
        auto es = ectx()->schemaManager()->getEdgeSchema(space, std::abs(edgeType));
        if (es == nullptr) {
            return Status::Error("No edge schema for %s", pair.first.c_str());
        }
        if (es->getFieldIndex(propName) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                    propName.c_str(), pair.first.c_str());
        }
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

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        return status;
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
        doError(std::move(status).status());
        return;
    }
    auto returns = status.value();
    std::string filterPushdown = "";
    if (FLAGS_filter_pushdown && isFinalStep()
            && direction_ == OverClause::Direction::kForward) {
        // TODO: not support filter pushdown in reversely traversal now.
        filterPushdown = whereWrapper_->filterPushdown_;
    }
    VLOG(1) << "edge type size: " << edgeTypes_.size()
            << " return cols: " << returns.size();
    auto future  = ectx()->getStorageClient()->getNeighbors(spaceId,
                                                            starts_,
                                                            edgeTypes_,
                                                            filterPushdown,
                                                            std::move(returns));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get neighbors failed"));
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
        if (FLAGS_trace_go) {
            LOG(INFO) << "Step:" << curStep_
                      << " finished, total request vertices " << starts_.size();
            auto& hostLatency = result.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); i++) {
                LOG(INFO) << std::get<0>(hostLatency[i])
                          << ", time cost " << std::get<1>(hostLatency[i])
                          << "us / " << std::get<2>(hostLatency[i])
                          << "us, total results " << result.responses()[i].get_vertices()->size();
            }
        }
        onStepOutResponse(std::move(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle out-bounds/in-bounds: " << e.what();
        doError(Status::Error("Exception when handle out-bounds/in-bounds: %s.",
                    e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

#define GO_EXIT() do { \
        if (!isRecord()) { \
            onEmptyInputs(); \
            return; \
        } else { \
            maybeFinishExecution(); \
            return; \
        } \
    } while (0);

void GoExecutor::onStepOutResponse(RpcResponse &&rpcResp) {
    joinResp(std::move(rpcResp));

    // back trace each step
    CHECK_GT(records_.size(), 0);
    auto dsts = getDstIdsFromRespWithBackTrack(records_.back());
    if (isFinalStep()) {
        GO_EXIT();
    } else {
        starts_ = std::move(dsts);
        if (starts_.empty()) {
            GO_EXIT();
        }
        curStep_++;
        stepOut();
    }
}

#undef GO_EXIT

void GoExecutor::maybeFinishExecution() {
    auto requireDstProps = expCtx_->hasDstTagProp();

    // Non-reversely traversal, no properties required on destination nodes
    // Or, Reversely traversal but no properties on edge and destination nodes required.
    // Note that the `dest` which used in reversely traversal means the `src` in foword edge.
    if (!requireDstProps) {
        finishExecution();
        return;
    }

    CHECK_GT(recordFrom_, 0);
    CHECK_GE(records_.size(), recordFrom_ - 1) << "Current step " << curStep_;
    auto dstIds = getDstIdsFromResps(records_.begin() + recordFrom_ - 1, records_.end());

    // Reaching the dead end
    if (dstIds.empty()) {
        onEmptyInputs();
        return;
    }

    DCHECK(requireDstProps);
    // Only properties on destination nodes required
    fetchVertexProps(std::move(dstIds));
    return;
}

void GoExecutor::onVertexProps(RpcResponse &&rpcResp) {
    UNUSED(rpcResp);
}

std::vector<VertexID> GoExecutor::getDstIdsFromResps(std::vector<RpcResponse>::iterator begin,
                                                     std::vector<RpcResponse>::iterator end) const {
    std::unordered_set<VertexID> set;
    for (auto it = begin; it != end; ++it) {
        for (const auto &resp : it->responses()) {
            auto *vertices = resp.get_vertices();
            if (vertices == nullptr) {
                continue;
            }

            for (const auto &vdata : *vertices) {
                for (const auto &edata : vdata.edge_data) {
                    for (const auto& edge : edata.get_edges()) {
                        auto dst = edge.get_dst();
                        set.emplace(dst);
                    }
                }
            }
        }
    }
    return std::vector<VertexID>(set.begin(), set.end());
}

std::vector<VertexID> GoExecutor::getDstIdsFromRespWithBackTrack(const RpcResponse &rpcResp) const {
    // back trace in current step
    // To avoid overlap in on step edges
    // For example
    // Dst , Src
    // 6  ,  1
    // 7  ,  6
    // Will mistake lead to 7->6 if insert one by one
    // So read all roots of current step first , then insert them
    std::multimap<VertexID, VertexID> backTrace;
    std::unordered_set<VertexID> set;
    for (const auto &resp : rpcResp.responses()) {
        auto *vertices = resp.get_vertices();
        if (vertices == nullptr) {
            continue;
        }

        for (const auto &vdata : *vertices) {
            for (const auto &edata : vdata.edge_data) {
                for (const auto& edge : edata.get_edges()) {
                    auto dst = edge.get_dst();
                    if (!isFinalStep() && backTracker_ != nullptr) {
                        auto range = backTracker_->get(vdata.get_vertex_id());
                        if (range.first == range.second) {  // not found root
                            backTrace.emplace(dst, vdata.get_vertex_id());
                        }
                        for (auto trace = range.first; trace != range.second; ++trace) {
                            backTrace.emplace(dst, trace->second);
                        }
                    }
                    set.emplace(dst);
                }
            }
        }
    }
    if (!isFinalStep() && backTracker_ != nullptr) {
        backTracker_->inject(backTrace);
    }
    return std::vector<VertexID>(set.begin(), set.end());
}

void GoExecutor::finishExecution() {
    // MayBe we can do better.
    std::vector<std::unique_ptr<YieldColumn>> yc;
    if (expCtx_->isOverAllEdge() && yields_.empty()) {
        for (const auto &alias : expCtx_->getEdgeAlias()) {
            auto dummy = new std::string(alias);
            auto dummy_exp = new EdgeDstIdExpression(dummy);
            auto ptr = std::make_unique<YieldColumn>(dummy_exp);
            dummy_exp->setContext(expCtx_.get());
            yields_.emplace_back(ptr.get());
            yc.emplace_back(std::move(ptr));
        }
    }


    if (onResult_) {
        std::unique_ptr<InterimResult> outputs;
        if (!setupInterimResult(outputs)) {
            return;
        }
        onResult_(std::move(outputs));
    } else {
        auto start = time::WallClock::fastNowInMicroSec();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(getResultColumnNames());
        auto ret = toThriftResponse();
        if (FLAGS_trace_go) {
            LOG(INFO) << "Process the resp from storaged, total time "
                      << time::WallClock::fastNowInMicroSec() - start << "us";
        }
        if (!ret.ok()) {
            LOG(ERROR) << "Get rows failed: " << ret.status();
            return;
        }
        if (!ret.value().empty()) {
            resp_->set_rows(std::move(ret).value());
        }
    }
    doFinish(Executor::ProcessControl::kNext);
}

StatusOr<std::vector<cpp2::RowValue>> GoExecutor::toThriftResponse() const {
    std::vector<cpp2::RowValue> rows;
    int64_t totalRows = 0;
    CHECK_GT(recordFrom_, 0);
    for (auto rpcResp = records_.begin() + recordFrom_ - 1; rpcResp != records_.end(); ++rpcResp) {
        for (const auto& resp : rpcResp->responses()) {
            if (resp.get_total_edges() != nullptr) {
                totalRows += *resp.get_total_edges();
            }
        }
    }
    rows.reserve(totalRows);
    auto cb = [&] (std::vector<VariantType> record,
                   const std::vector<nebula::cpp2::SupportedType>& colTypes) -> Status {
        std::vector<cpp2::ColumnValue> row;
        row.reserve(record.size());
        for (size_t i = 0; i < colTypes.size(); i++) {
            auto& column = record[i];
            auto& type = colTypes[i];
            row.emplace_back();
            switch (type) {
                case nebula::cpp2::SupportedType::BOOL:
                    row.back().set_bool_val(boost::get<bool>(column));
                    break;
                case nebula::cpp2::SupportedType::INT:
                    row.back().set_integer(boost::get<int64_t>(column));
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    row.back().set_double_precision(boost::get<double>(column));
                    break;
                case nebula::cpp2::SupportedType::FLOAT:
                    row.back().set_single_precision(boost::get<double>(column));
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    row.back().set_str(boost::get<std::string>(column));
                    break;
                case nebula::cpp2::SupportedType::TIMESTAMP:
                    row.back().set_timestamp(boost::get<int64_t>(column));
                    break;
                case nebula::cpp2::SupportedType::VID:
                    row.back().set_id(boost::get<int64_t>(column));
                    break;
                default:
                    {
                        switch (column.which()) {
                        case VAR_INT64:
                            row.back().set_integer(boost::get<int64_t>(column));
                            break;
                        case VAR_DOUBLE:
                            row.back().set_double_precision(boost::get<double>(column));
                            break;
                        case VAR_BOOL:
                            break;
                        case VAR_STR:
                            row.back().set_str(boost::get<std::string>(column));
                            break;
                        default:
                            LOG(FATAL) << "Unknown VariantType: " << column.which();
                        }
                    }
                    break;
            }
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        return Status::OK();
    };  // cb

    if (!processFinalResult(cb)) {
        return Status::Error("process failed");
    }
    if (FLAGS_trace_go) {
        LOG(INFO) << "Total rows:" << rows.size();
    }
    return rows;
}

StatusOr<std::vector<storage::cpp2::PropDef>> GoExecutor::getStepOutProps() {
    std::vector<storage::cpp2::PropDef> props;
    if (!isRecord()) {
        for (auto &e : edgeTypes_) {
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name = _DST;
            pd.id.set_edge_type(e);
            props.emplace_back(std::move(pd));
        }
        return props;
    } else {
        for (auto &e : edgeTypes_) {
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name = _DST;
            pd.id.set_edge_type(e);
            props.emplace_back(std::move(pd));
            VLOG(3) << "Need edge props: " << e << ", _dst";
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
            VLOG(3) << "Need tag src props: " << tagProp.first << ", " << tagProp.second;
        }
        for (auto &prop : expCtx_->aliasProps()) {
            if (prop.second == _DST) {
                continue;
            }
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name  = prop.second;

            EdgeType edgeType;

            if (!expCtx_->getEdgeType(prop.first, edgeType)) {
                return Status::Error("the edge was not found '%s'", prop.first.c_str());
            }
            pd.id.set_edge_type(edgeType);
            switch (direction_) {
                case OverClause::Direction::kForward: {
                    props.emplace_back(std::move(pd));
                    break;
                }
                case OverClause::Direction::kBackward: {
                    edgeType = -edgeType;
                    pd.id.set_edge_type(edgeType);
                    props.emplace_back(std::move(pd));
                    break;
                }
                case OverClause::Direction::kBidirect: {
                    props.emplace_back(pd);
                    edgeType = -edgeType;
                    pd.id.set_edge_type(edgeType);
                    props.emplace_back(std::move(pd));
                    break;
                }
                default:
                    return Status::Error(
                            "Unknown direction: %ld", static_cast<int64_t>(direction_));
            }
            VLOG(3) << "Need edge props: " << prop.first << ", " << prop.second;
        }
        return props;
    }
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
        VLOG(3) << "Need dst tag props: " << tagProp.first << ", " << tagProp.second;
    }
    return props;
}


void GoExecutor::fetchVertexProps(std::vector<VertexID> ids) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getDstProps();
    if (!status.ok()) {
        doError(std::move(status).status());
        return;
    }
    auto returns = status.value();
    auto future = ectx()->getStorageClient()->getVertexProps(spaceId, ids, returns);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get dest props failed"));
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
        finishExecution();
        return;
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when get vertex in go: " << e.what();
        doError(Status::Error("Exception when get vertex in go: %s.",
                    e.what().c_str()));
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


bool GoExecutor::setupInterimResult(std::unique_ptr<InterimResult> &result) const {
    // Generic results
    result = std::make_unique<InterimResult>(getResultColumnNames());
    std::shared_ptr<SchemaWriter> schema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto cb = [&] (std::vector<VariantType> record,
                   const std::vector<nebula::cpp2::SupportedType>& colTypes) -> Status {
        if (schema == nullptr) {
            schema = std::make_shared<SchemaWriter>();
            auto colnames = getResultColumnNames();
            if (record.size() != colTypes.size()) {
                LOG(ERROR) << "Record size: " << record.size()
                           << " != column type size: " << colTypes.size();
                return Status::Error("Record size is not equal to column type size, [%lu != %lu]",
                                      record.size(), colTypes.size());
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
                            LOG(ERROR) << "Unknown VariantType: " << record[i].which();
                            return Status::Error("Unknown VariantType: %d", record[i].which());
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
                    LOG(ERROR) << "Unknown VariantType: " << column.which();
                    return Status::Error("Unknown VariantType: %d", column.which());
            }
        }

        rsWriter->addRow(writer.encode());
        return Status::OK();
    };  // cb

    if (!processFinalResult(cb)) {
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
    doFinish(Executor::ProcessControl::kNext);
}

bool GoExecutor::processFinalResult(Callback cb) const {
    const bool joinInput = yieldInput();
    auto uniqResult = std::make_unique<std::unordered_set<size_t>>();
    auto spaceId = ectx()->rctx()->session()->space();
    std::vector<SupportedType> colTypes;
    for (auto *column : yields_) {
        colTypes.emplace_back(calculateExprType(column->expr()));
    }
    std::size_t recordIn = recordFrom_;
    CHECK_GT(recordFrom_, 0);
    for (auto rpcResp = records_.begin() + recordFrom_ - 1;
         rpcResp != records_.end();
         ++rpcResp, ++recordIn) {
        const auto& all = rpcResp->responses();

        std::vector<VariantType> record;
        record.reserve(yields_.size());
        for (const auto &resp : all) {
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
            VLOG(1) << "Total resp.vertices size " << resp.vertices.size();
            for (const auto &vdata : resp.vertices) {
                DCHECK(vdata.__isset.edge_data);
                VLOG(1) << "Total vdata.edge_data size " << vdata.edge_data.size();
                auto tagData = vdata.get_tag_data();
                auto srcId = vdata.get_vertex_id();
                const auto roots = getRoots(srcId, recordIn);
                auto inputRows = index_->rowsOfVids(roots);
                // Here if join the input we extend the input rows coresponding to current vertex;
                // Or just loop once as previous that not join anything,
                // in fact result what in responses.
                bool notJoinOnce = false;
                for (auto inputRow = inputRows.begin();
                    !joinInput || inputRow != inputRows.end();
                    joinInput ? ++inputRow : inputRow) {
                    if (!joinInput) {
                        if (notJoinOnce) {
                            break;
                        }
                        notJoinOnce = true;
                    }
                    for (const auto &edata : vdata.edge_data) {
                        auto edgeType = edata.type;
                        VLOG(1) << "Total edata.edges size " << edata.edges.size()
                                << ", for edge " << edgeType;
                        std::shared_ptr<ResultSchemaProvider> currEdgeSchema;
                        auto it = edgeSchema.find(edgeType);
                        if (it != edgeSchema.end()) {
                            currEdgeSchema = it->second;
                        }
                        VLOG(1) << "CurrEdgeSchema is null? " << (currEdgeSchema == nullptr);
                        for (const auto& edge : edata.edges) {
                            auto dstId = edge.get_dst();
                            Getters getters;
                            getters.getEdgeDstId = [this,
                                                    &dstId,
                                                    &edgeType] (const std::string& edgeName)
                                                                    -> OptVariantType {
                                if (edgeTypes_.size() > 1) {
                                    EdgeType type;
                                    auto found = expCtx_->getEdgeType(edgeName, type);
                                    if (!found) {
                                        return Status::Error(
                                                "Get edge type for `%s' failed in getters.",
                                                edgeName.c_str());
                                    }
                                    if (type != std::abs(edgeType)) {
                                        return 0L;
                                    }
                                }
                                return dstId;
                            };
                            getters.getSrcTagProp = [&spaceId,
                                                    &tagData,
                                                    &tagSchema,
                                                    this] (const std::string &tag,
                                                       const std::string &prop) -> OptVariantType {
                                TagID tagId;
                                auto found = expCtx_->getTagId(tag, tagId);
                                if (!found) {
                                    return Status::Error(
                                            "Get tag id for `%s' failed in getters.", tag.c_str());
                                }

                                auto it2 = std::find_if(tagData.cbegin(),
                                                        tagData.cend(),
                                                        [&tagId] (auto &td) {
                                    if (td.tag_id == tagId) {
                                        return true;
                                    }
                                    return false;
                                });
                                if (it2 == tagData.cend()) {
                                    auto ts = ectx()->schemaManager()->getTagSchema(spaceId, tagId);
                                    if (ts == nullptr) {
                                        return Status::Error("No tag schema for %s", tag.c_str());
                                    }
                                    return RowReader::getDefaultProp(ts.get(), prop);
                                }
                                DCHECK(it2->__isset.data);
                                auto vreader = RowReader::getRowReader(it2->data, tagSchema[tagId]);
                                auto res = RowReader::getPropByName(vreader.get(), prop);
                                if (!ok(res)) {
                                    return Status::Error("get prop(%s.%s) failed",
                                                         tag.c_str(),
                                                         prop.c_str());
                                }
                                return value(res);
                            };
                            // In reverse mode, it is used to get the src props.
                            getters.getDstTagProp = [&spaceId,
                                                    &dstId,
                                                    this] (const std::string &tag,
                                                        const std::string &prop) -> OptVariantType {
                                TagID tagId;
                                auto found = expCtx_->getTagId(tag, tagId);
                                if (!found) {
                                    return Status::Error(
                                            "Get tag id for `%s' failed in getters.", tag.c_str());
                                }
                                auto ret = vertexHolder_->get(dstId, tagId, prop);
                                if (!ret.ok()) {
                                    auto ts = ectx()->schemaManager()->getTagSchema(spaceId, tagId);
                                    if (ts == nullptr) {
                                        return Status::Error("No tag schema for %s", tag.c_str());
                                    }
                                    return RowReader::getDefaultProp(ts.get(), prop);
                                }
                                return ret.value();
                            };
                            getters.getVariableProp = [inputRow, this] (const std::string &prop) {
                                return index_->getColumnWithRow(*inputRow, prop);
                            };

                            getters.getInputProp = [inputRow, this] (const std::string &prop) {
                                return index_->getColumnWithRow(*inputRow, prop);
                            };

                            std::unique_ptr<RowReader> reader;
                            if (currEdgeSchema) {
                                reader = RowReader::getRowReader(edge.props, currEdgeSchema);
                            }

                            // In reverse mode, we should handle _src
                            getters.getAliasProp = [&reader,
                                                    &srcId,
                                                    &edgeType,
                                                    &edgeSchema,
                                                    this] (const std::string &edgeName,
                                                        const std::string &prop) mutable
                                                                        -> OptVariantType {
                                EdgeType type;
                                auto found = expCtx_->getEdgeType(edgeName, type);
                                if (!found) {
                                    return Status::Error(
                                        "Get edge type for `%s' failed in getters.",
                                        edgeName.c_str());
                                }
                                if (std::abs(edgeType) != type) {
                                    auto sit = edgeSchema.find(
                                        direction_ ==
                                        OverClause::Direction::kBackward ? -type : type);
                                    if (sit == edgeSchema.end()) {
                                        std::string errMsg = folly::stringPrintf(
                                                "Can't find shcema for %s when get default.",
                                                edgeName.c_str());
                                        LOG(ERROR) << errMsg;
                                        return Status::Error(errMsg);
                                    }
                                    return RowReader::getDefaultProp(sit->second.get(), prop);
                                }

                                if (prop == _SRC) {
                                    return srcId;
                                }
                                DCHECK(reader != nullptr);
                                auto res = RowReader::getPropByName(reader.get(), prop);
                                if (!ok(res)) {
                                    LOG(ERROR) << "Can't get prop for " << prop
                                            << ", edge " << edgeName;
                                    return Status::Error(
                                            folly::sformat("get prop({}.{}) failed",
                                                        edgeName,
                                                        prop));
                                }
                                return value(std::move(res));
                            };  // getAliasProp
                            // Evaluate filter
                            if (whereWrapper_->filter_ != nullptr) {
                                auto value = whereWrapper_->filter_->eval(getters);
                                if (!value.ok()) {
                                    doError(std::move(value).status());
                                    return false;
                                }
                                if (!Expression::asBool(value.value())) {
                                    continue;
                                }
                            }
                            record.clear();
                            for (auto *column : yields_) {
                                auto *expr = column->expr();
                                auto value = expr->eval(getters);
                                if (!value.ok()) {
                                    doError(std::move(value).status());
                                    return false;
                                }
                                record.emplace_back(std::move(value.value()));
                            }
                            // Check if duplicate
                            if (distinct_) {
                                auto ret = uniqResult->emplace(boost::hash_range(record.begin(),
                                                                                record.end()));
                                if (!ret.second) {
                                    continue;
                                }
                            }
                            auto cbStatus = cb(std::move(record), colTypes);
                            if (!cbStatus.ok()) {
                                LOG(ERROR) << cbStatus;
                                doError(std::move(cbStatus));
                                return false;
                            }
                        }  // for edges
                    }  // for edata
                }   // for input rows
            }  // for vdata
        }   // for `resp'
    }
    return true;
}

OptVariantType GoExecutor::VertexHolder::getDefaultProp(TagID tid, const std::string &prop) const {
    for (auto it = data_.cbegin(); it != data_.cend(); ++it) {
        auto it2 = it->second.find(tid);
        if (it2 != it->second.cend()) {
            return RowReader::getDefaultProp(std::get<0>(it2->second).get(), prop);
        }
    }


    return Status::Error("Unknown property: `%s'", prop.c_str());
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

void GoExecutor::joinResp(RpcResponse &&resp) {
    records_.emplace_back(std::move(resp));
}

}   // namespace graph
}   // namespace nebula
