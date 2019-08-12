/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GoExecutor.h"
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
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    status = setupStarts();
    if (!status.ok()) {
        onError_(std::move(status));
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
            break;
        }

        auto vidList = clause->vidList();
        for (auto *expr : vidList) {
            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval();
            if (!value.ok()) {
                status = Status::Error();
                break;
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


Status GoExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    do {
        if (clause == nullptr) {
            LOG(FATAL) << "Over clause shall never be null";
        }
        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *clause->edge());
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        edgeType_ = edgeStatus.value();
        reversely_ = clause->isReversely();
    } while (false);

    if (isReversely()) {
        return Status::Error("`REVERSELY' not supported yet");
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
    if (clause != nullptr) {
        yields_ = clause->columns();
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
    if (inputs == nullptr) {
        return Status::OK();
    }

    auto result = inputs->getVIDs(*colname_);
    if (!result.ok()) {
        return std::move(result).status();
    }
    starts_ = std::move(result).value();
    index_ = inputs->buildIndex(*colname_);
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
        DCHECK(onError_);
        onError_(Status::Error("Get step out props failed"));
        return;
    }
    auto returns = status.value();
    auto future = ectx()->storage()->getNeighbors(spaceId,
                                                  starts_,
                                                  edgeType_,
                                                  !reversely_,
                                                  "",
                                                  std::move(returns));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            DCHECK(onError_);
            onError_(Status::Error("Get neighbors failed"));
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
        onError_(Status::Error("Internal error"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void GoExecutor::onStepOutResponse(RpcResponse &&rpcResp) {
    if (isFinalStep()) {
        if (expCtx_->hasDstTagProp()) {
            auto dstids = getDstIdsFromResp(rpcResp);
            if (dstids.empty()) {
                onEmptyInputs();
                return;
            }
            fetchVertexProps(std::move(dstids), std::move(rpcResp));
            return;
        }
        finishExecution(std::move(rpcResp));
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


void GoExecutor::onVertexProps(RpcResponse &&rpcResp) {
    UNUSED(rpcResp);
}


std::vector<VertexID> GoExecutor::getDstIdsFromResp(RpcResponse &rpcResp) const {
    std::unordered_set<VertexID> set;
    for (auto &resp : rpcResp.responses()) {
        auto *vertices = resp.get_vertices();
        if (vertices == nullptr) {
            continue;
        }
        auto schema = std::make_shared<ResultSchemaProvider>(resp.edge_schema);
        for (auto &vdata : *vertices) {
            RowSetReader rsReader(schema, vdata.edge_data);
            auto iter = rsReader.begin();
            while (iter) {
                VertexID dst;
                auto rc = iter->getVid("_dst", dst);
                CHECK(rc == ResultType::SUCCEEDED);
                if (!isFinalStep() && backTracker_ != nullptr) {
                    backTracker_->add(vdata.get_vertex_id(), dst);
                }
                set.emplace(dst);
                ++iter;
            }
        }
    }
    return std::vector<VertexID>(set.begin(), set.end());
}

void GoExecutor::finishExecution(RpcResponse &&rpcResp) {
    std::unique_ptr<InterimResult> outputs;

    if (!setupInterimResult(std::move(rpcResp), outputs)) {
        return;
    }
    if (onResult_) {
        onResult_(std::move(outputs));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(getResultColumnNames());

        if (outputs != nullptr) {
            auto rows = outputs->getRows();
            resp_->set_rows(std::move(rows));
        }
    }
    DCHECK(onFinish_);
    onFinish_();
}

StatusOr<std::vector<storage::cpp2::PropDef>> GoExecutor::getStepOutProps() {
    std::vector<storage::cpp2::PropDef> props;
    {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = "_dst";
        props.emplace_back(std::move(pd));
    }

    if (!isFinalStep()) {
        return props;
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


StatusOr<std::vector<storage::cpp2::PropDef>> GoExecutor::getDstProps() {
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


void GoExecutor::fetchVertexProps(std::vector<VertexID> ids, RpcResponse &&rpcResp) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getDstProps();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(Status::Error("Get dest props failed"));
        return;
    }
    auto returns = status.value();
    auto future = ectx()->storage()->getVertexProps(spaceId, ids, returns);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, stepOutResp = std::move(rpcResp)] (auto &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            DCHECK(onError_);
            onError_(Status::Error("Get dest props failed"));
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
        onError_(Status::Error("Internal error"));
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
    std::shared_ptr<SchemaWriter> schema;
    std::unique_ptr<RowSetWriter> rsWriter;
    auto uniqResult = std::make_unique<std::unordered_set<std::string>>();
    auto cb = [&] (std::vector<VariantType> record) {
        if (schema == nullptr) {
            schema = std::make_shared<SchemaWriter>();
            auto colnames = getResultColumnNames();
            for (auto i = 0u; i < record.size(); i++) {
                SupportedType type;
                switch (record[i].which()) {
                    case 0:
                        // all integers in InterimResult are regarded as type of VID
                        type = SupportedType::VID;
                        break;
                    case 1:
                        type = SupportedType::DOUBLE;
                        break;
                    case 2:
                        type = SupportedType::BOOL;
                        break;
                    case 3:
                        type = SupportedType::STRING;
                        break;
                    default:
                        LOG(FATAL) << "Unknown VariantType: " << record[i].which();
                }
                schema->appendCol(colnames[i], type);
            }  // for
            rsWriter = std::make_unique<RowSetWriter>(schema);
        }  // if

        RowWriter writer(schema);
        for (auto &column : record) {
            switch (column.which()) {
                case 0:
                    writer << boost::get<int64_t>(column);
                    break;
                case 1:
                    writer << boost::get<double>(column);
                    break;
                case 2:
                    writer << boost::get<bool>(column);
                    break;
                case 3:
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
    // No results populated
    if (rsWriter != nullptr) {
        result = std::make_unique<InterimResult>(std::move(rsWriter));
    }
    return true;
}


void GoExecutor::onEmptyInputs() {
    if (onResult_) {
        onResult_(nullptr);
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    onFinish_();
}


bool GoExecutor::processFinalResult(RpcResponse &rpcResp, Callback cb) const {
    auto all = rpcResp.responses();
    for (auto &resp : all) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        std::shared_ptr<ResultSchemaProvider> vschema;
        std::shared_ptr<ResultSchemaProvider> eschema;
        if (resp.get_vertex_schema() != nullptr) {
            vschema = std::make_shared<ResultSchemaProvider>(resp.vertex_schema);
        }
        if (resp.get_edge_schema() != nullptr) {
            eschema = std::make_shared<ResultSchemaProvider>(resp.edge_schema);
        }

        for (auto &vdata : resp.vertices) {
            std::unique_ptr<RowReader> vreader;
            if (vschema != nullptr) {
                DCHECK(vdata.__isset.vertex_data);
                vreader = RowReader::getRowReader(vdata.vertex_data, vschema);
            }
            DCHECK(vdata.__isset.edge_data);
            DCHECK(eschema != nullptr);
            RowSetReader rsReader(eschema, vdata.edge_data);
            auto iter = rsReader.begin();
            while (iter) {
                auto &getters = expCtx_->getters();
                getters.getAliasProp = [&](const std::string &,
                                           const std::string &prop) -> OptVariantType {
                    auto res = RowReader::getPropByName(&*iter, prop);
                    if (ok(res)) {
                        return value(res);
                    }
                    return Status::Error("get edge prop failed");
                };
                getters.getSrcTagProp = [&](const std::string &tagName,
                                            const std::string &prop) -> OptVariantType {
                    auto tagIter = this->srcTagProps_.find(std::make_pair(tagName, prop));
                    if (tagIter == this->srcTagProps_.end()) {
                        auto msg = folly::sformat(
                            "Src tagName : {} , propName : {} is not exist", tagName, prop);
                        LOG(ERROR) << msg;
                        return Status::Error(msg);
                    }
                    auto index = tagIter->second;
                    const nebula::cpp2::ValueType &type = vschema->getFieldType(index);
                    if (type == CommonConstants::kInvalidValueType()) {
                        auto msg =
                            folly::sformat("Tag: {} no schema for the index {}", tagName, index);
                        LOG(ERROR) << msg;
                        return Status::Error(msg);
                    }
                    auto res = RowReader::getPropByIndex(vreader.get(), index);
                    if (ok(res)) {
                        return value(std::move(res));
                    }
                    return Status::Error(folly::sformat("{}.{} was not exist", tagName, prop));
                };
                getters.getDstTagProp = [&](const std::string &tagName,
                                            const std::string &prop) -> OptVariantType {
                    auto res = RowReader::getPropByName(&*iter, "_dst");
                    CHECK(ok(res));
                    auto dst = value(std::move(res));
                    auto tagIter = this->dstTagProps_.find(std::make_pair(tagName, prop));
                    if (tagIter == this->dstTagProps_.end()) {
                        auto msg = folly::sformat(
                            "Src tagName : {} , propName : {} is not exist", tagName, prop);
                        LOG(ERROR) << msg;
                        return Status::Error(msg);
                    }
                    auto index = tagIter->second;
                    return vertexHolder_->get(boost::get<int64_t>(dst), index);
                };
                getters.getVariableProp = [&] (const std::string &prop) {
                    return getPropFromInterim(vdata.get_vertex_id(), prop);
                };
                getters.getInputProp = [&] (const std::string &prop) {
                    return getPropFromInterim(vdata.get_vertex_id(), prop);
                };
                // Evaluate filter
                if (filter_ != nullptr) {
                    auto value = filter_->eval();
                    if (!value.ok()) {
                        onError_(value.status());
                        return false;
                    }
                    if (!Expression::asBool(value.value())) {
                        ++iter;
                        continue;
                    }
                }
                std::vector<VariantType> record;
                record.reserve(yields_.size());
                for (auto *column : yields_) {
                    auto *expr = column->expr();
                    auto value = expr->eval();
                    if (!value.ok()) {
                        onError_(value.status());
                        return false;
                    }
                    record.emplace_back(std::move(value.value()));
                }
                cb(std::move(record));
                ++iter;
            }   // while `iter'
        }   // for `vdata'
    }   // for `resp'
    return true;
}


OptVariantType GoExecutor::VertexHolder::get(VertexID id, int64_t index) const {
    DCHECK(schema_ != nullptr);
    auto iter = data_.find(id);

    // TODO(dutor) We need a type to represent NULL
    CHECK(iter != data_.end());

    auto reader = RowReader::getRowReader(iter->second, schema_);

    auto res = RowReader::getPropByIndex(reader.get(), index);
    CHECK(ok(res));
    return value(std::move(res));
}


void GoExecutor::VertexHolder::add(const storage::cpp2::QueryResponse &resp) {
    auto *vertices = resp.get_vertices();
    if (vertices == nullptr) {
        return;
    }

    if (vertices->empty()) {
        return;
    }

    if (resp.get_vertex_schema() == nullptr) {
        return;
    }

    if (schema_ == nullptr) {
        schema_ = std::make_shared<ResultSchemaProvider>(resp.vertex_schema);
    }

    for (auto &vdata : *vertices) {
        DCHECK(vdata.__isset.vertex_data);
        if (!vdata.vertex_data.empty()) {
            data_[vdata.vertex_id] = vdata.vertex_data;
        }
    }
}


VariantType GoExecutor::getPropFromInterim(VertexID id, const std::string &prop) const {
    auto rootId = id;
    if (backTracker_ != nullptr) {
        DCHECK_NE(steps_ , 1u);
        rootId = backTracker_->get(id);
    }
    return index_->getColumnWithVID(rootId, prop);
}

}   // namespace graph
}   // namespace nebula
