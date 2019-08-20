/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "FindPathExecutor.h"

namespace nebula {
namespace graph {
using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;

FindPathExecutor::FindPathExecutor(Sentence *sentence, ExecutionContext *exct)
        : TraverseExecutor(exct) {
    sentence_ = static_cast<FindPathSentence*>(sentence);
}

Status FindPathExecutor::prepare() {
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    do {
        status = prepareFrom();
        if (!status.ok()) {
            break;
        }
        status = prepareTo();
        if (!status.ok()) {
            break;
        }
        status = prepareOver();
        if (!status.ok()) {
            break;
        }
        status = prepareStep();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status FindPathExecutor::prepareFrom() {
    auto *clause = sentence_->from();
    if (clause->isRef()) {
        prepareVids(clause->ref(), from_);
    }
    return Status::OK();
}

Status FindPathExecutor::prepareTo() {
    auto *clause = sentence_->to();
    if (clause->isRef()) {
        prepareVids(clause->ref(), to_);
    }
    return Status::OK();
}

void FindPathExecutor::prepareVids(Expression *expr, Vertices &vertices) {
    if (expr->isInputExpression()) {
        auto *iexpr = static_cast<InputPropertyExpression*>(expr);
        vertices.colname_ = iexpr->prop();
    } else if (expr->isVariableExpression()) {
        auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
        vertices.varname_ = vexpr->alias();
        vertices.colname_ = vexpr->prop();
    } else {
        //  should never come to here.
        //  only support input and variable yet.
        LOG(FATAL) << "Unknown kind of expression.";
    }
}

Status FindPathExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->over();
    do {
        if (clause == nullptr) {
            LOG(FATAL) << "Over clause shall never be null";
        }
        // TODO move this to execute
        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *clause->edge());
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        over_.edge_ = clause->edge();
        over_.edgeType_ = edgeStatus.value();
        over_.reversely_ = clause->isReversely();
    } while (false);

    return status;
}

Status FindPathExecutor::prepareStep() {
    auto *clause = sentence_->step();
    if (clause != nullptr) {
        step_.steps_ = clause->steps();
        step_.upto_ = clause->isUpto();
    }
    return Status::OK();
}

Status FindPathExecutor::prepareWhere() {
    auto *clause = sentence_->where();
    if (clause != nullptr) {
        auto *filter = clause->filter();
        filter->setContext(expCtx_.get());
        auto status = filter->prepare();
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

void FindPathExecutor::execute() {
    DCHECK(onError_);
    DCHECK(onFinish_);
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }
    spaceId_ = ectx()->rctx()->session()->space();

    status = setupVids();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }

    auto *runner = ectx()->rctx()->runner();
    auto step = step_.steps_ / 2 + step_.steps_ % 2;
    std::vector<VertexID> fromVids = from_.vids_;
    std::vector<VertexID> toVids = to_.vids_;
    while (step && !stop_) {
        folly::Promise<std::vector<VertexID>> proF;
        addGoFromFTask(std::move(fromVids), proF);

        folly::Promise<std::vector<VertexID>> proT;
        addGoFromTTask(std::move(toVids), proT);

        std::vector<folly::Future<std::vector<VertexID>>> results;
        results.reserve(2);
        results.emplace_back(proF.getFuture());
        results.emplace_back(proT.getFuture());

        auto cb = [this] (auto &&result) { findPath(std::move(result)); };
        folly::collectAll(results).via(runner).thenValue(cb);
        --step;
    }
}

void FindPathExecutor::addGoFromFTask(std::vector<VertexID> &&fromVids,
                                      folly::Promise<std::vector<VertexID>> &proF) {
    auto *runner = ectx()->rctx()->runner();
    runner->add([this, vids = std::move(fromVids), &proF] () mutable {
        std::vector<VertexID> frontierF;
        auto props = getStepOutProps("_dst");
        if (!props.ok()) {
            folly::FutureException e(std::move(props).status().toString());
            proF.setException(e);
            return;
        }
        auto s = goFromF(std::move(vids), std::move(props).value(), frontierF);
        if (!s.ok()) {
            folly::FutureException e(std::move(s).toString());
            proF.setException(e);
            return;
        }
        proF.setValue(std::move(frontierF));
    });
}

void FindPathExecutor::addGoFromTTask(std::vector<VertexID> &&toVids,
                                      folly::Promise<std::vector<VertexID>> &proT) {
    auto *runner = ectx()->rctx()->runner();
    runner->add([this, vids = std::move(toVids), &proT] () mutable {
        std::vector<VertexID> frontierT;
        auto s = goFromT(std::move(vids), frontierT);
        if (!s.ok()) {
            folly::FutureException e(std::move(s).toString());
            proT.setException(e);
            return;
        }
        proT.setValue(std::move(frontierT));
    });
}

void FindPathExecutor::findPath(std::vector<folly::Try<std::vector<VertexID>>> &&result) {
    for (auto &t : result) {
        if (t.hasException()) {
            auto &e = t.exception();
            LOG(ERROR) << "Find path error: " << e.what();
            onError_(Status::Error(e.what()));
            stop_ = true;
            return;
        }
    }
}

Status FindPathExecutor::setupVids() {
    Status status = Status::OK();
    do {
        if (sentence_->from()->isRef()) {
            status = setupVidsFromRef(from_);
            if (!status.ok()) {
                break;
            }
        } else {
            status = setupVidsFromExpr(sentence_->from()->vidList(), from_);
            if (!status.ok()) {
                break;
            }
        }

        if (sentence_->to()->isRef()) {
            status = setupVidsFromRef(to_);
            if (!status.ok()) {
                break;
            }
        } else {
            status = setupVidsFromExpr(sentence_->to()->vidList(), to_);
            if (!status.ok()) {
                break;
            }
        }
    } while (false);

    return status;
}

Status FindPathExecutor::setupVidsFromExpr(std::vector<Expression*> &&vidList, Vertices &vertices) {
    Status status = Status::OK();
    auto uniqID = std::make_unique<std::unordered_set<VertexID>>();
    vertices.vids_.reserve(vidList.size());
    for (auto *expr : vidList) {
        status = expr->prepare();
        if (!status.ok()) {
            break;
        }
        auto value = expr->eval();
        if (!value.ok()) {
            return value.status();
        }
        auto v = value.value();
        if (!Expression::isInt(v)) {
            status = Status::Error("Vertex ID should be of type integer");
            break;
        }

        auto valInt = Expression::asInt(v);
        auto result = uniqID->emplace(valInt);
        if (result.second) {
            vertices.vids_.emplace_back(valInt);
        }
    }

    return status;
}

Status FindPathExecutor::setupVidsFromRef(Vertices &vertices) {
    const InterimResult *inputs;
    if (vertices.varname_ == nullptr) {
        inputs = inputs_.get();
        if (inputs == nullptr) {
            return Status::OK();
        }
    } else {
        inputs = ectx()->variableHolder()->get(*(vertices.varname_));
        if (inputs == nullptr) {
            return Status::Error("Variable `%s' not defined", vertices.varname_->c_str());
        }
    }

    auto result = inputs->getDistinctVIDs(*(vertices.colname_));
    if (!result.ok()) {
        return std::move(result).status();
    }
    vertices.vids_ = std::move(result).value();
    return Status::OK();
}

Status FindPathExecutor::goFromF(std::vector<VertexID> vids,
                                                std::vector<storage::cpp2::PropDef> props,
                                                std::vector<VertexID> &frontiers) {
    Status status;
    auto future = ectx()->storage()->getNeighbors(spaceId_,
                                                  vids,
                                                  over_.edgeType_,
                                                  false,
                                                  "",
                                                  std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, &status, &frontiers] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            status = Status::Error("Get neighbors failed.");
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (where_.filter_ != nullptr) {
            auto ret = doFilter(std::move(result), where_.filter_);
            if (!ret.ok()) {
                status = Status::Error("Do filter failed.");
                return;
            }
            frontiers = std::move(ret).value();
        }
    };
    auto error = [this, &status] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        status = Status::Error("Internal error");
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
    return status;
}

StatusOr<std::vector<VertexID>>
FindPathExecutor::doFilter(storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
         Expression *filter) {
    UNUSED(result);
    UNUSED(filter);
    return Status::OK();
}

Status FindPathExecutor::goFromT(std::vector<VertexID> vids, std::vector<VertexID> &frontiers) {
    Status status;
    std::vector<VertexID> frontiersT;

    std::vector<storage::cpp2::PropDef> props;
    storage::cpp2::PropDef pd;
    pd.owner = storage::cpp2::PropOwner::EDGE;
    pd.name = "_dst";
    props.emplace_back(std::move(pd));
    auto clientFuture = ectx()->storage()->getNeighbors(spaceId_,
                                                  vids,
                                                  over_.edgeType_,
                                                  true,
                                                  "",
                                                  std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, &frontiersT, &status] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            status = Status::Error("Get neighbors failed.");
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        auto ret = doFilter(std::move(result), nullptr);
        if (!ret.ok()) {
            status = Status::Error("Do filter failed.");
            return;
        }
        frontiersT = std::move(ret).value();
    };
    auto error = [this, &status] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        status = Status::Error("Internal error");
    };
    std::move(clientFuture).via(runner).thenValue(cb).thenError(error);

    if (!status.ok()) {
        return status;
    }

    if (where_.filter_ != nullptr) {
        auto ret = getStepOutProps("_src");
        if (!ret.ok()) {
            status = std::move(status);
            return status;
        }
        status = goFromF(std::move(frontiersT), std::move(ret).value(), frontiers);
    } else {
        frontiers = std::move(frontiersT);
    }
    return status;
}

StatusOr<std::vector<storage::cpp2::PropDef>>
FindPathExecutor::getStepOutProps(std::string reserve) {
    std::vector<storage::cpp2::PropDef> props;
    {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = reserve;
        props.emplace_back(std::move(pd));
    }

    auto spaceId = ectx()->rctx()->session()->space();
    SchemaProps tagProps;
    for (auto &tagProp : expCtx_->srcTagProps()) {
        tagProps[tagProp.first].emplace_back(tagProp.second);
    }

    int64_t index = -1;
    for (auto &tagIt : tagProps) {
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagIt.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagIt.first);
        }
        auto tagId = status.value();
        for (auto &prop : tagIt.second) {
            index++;
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::DEST;
            pd.name = prop;
            pd.set_tag_id(tagId);
            props.emplace_back(std::move(pd));
            srcTagProps_.emplace(std::make_pair(tagIt.first, prop), index);
        }
    }

    for (auto &prop : expCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = prop.second;
        props.emplace_back(std::move(pd));
    }

    return props;
}


StatusOr<std::vector<storage::cpp2::PropDef>> FindPathExecutor::getDstProps() {
    auto spaceId = ectx()->rctx()->session()->space();
    SchemaProps tagProps;
    for (auto &tagProp : expCtx_->dstTagProps()) {
        tagProps[tagProp.first].emplace_back(tagProp.second);
    }

    std::vector<storage::cpp2::PropDef> props;
    int64_t index = -1;
    for (auto &tagIt : tagProps) {
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagIt.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagIt.first);
        }
        auto tagId = status.value();
        for (auto &prop : tagIt.second) {
            index++;
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::DEST;
            pd.name = prop;
            pd.set_tag_id(tagId);
            props.emplace_back(std::move(pd));
            dstTagProps_.emplace(std::make_pair(tagIt.first, prop), index);
        }
    }
    return props;
}
}  // namespace graph
}  // namespace nebula
