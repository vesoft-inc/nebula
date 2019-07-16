/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Status.h"
#include "graph/UpdateEdgeExecutor.h"
#include "meta/SchemaManager.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

// UPDATE [OR INSERT] EDGE <vertex_id> -> <vertex_id> [@<weight>] OF <edge_type>
// SET <update_decl> [WHERE <conditions>] [YIELD <field_list>]
UpdateEdgeExecutor::UpdateEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UpdateEdgeSentence*>(sentence);
}


Status UpdateEdgeExecutor::prepare() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();
    expCtx_ = std::make_unique<ExpressionContext>();
    yieldExpCtx_ = std::make_unique<ExpressionContext>();
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        insertable_ = sentence_->getInsertable();
        status = sentence_->getSrcId()->prepare();
        if (!status.ok()) {
            break;
        }
        auto src = sentence_->getSrcId()->eval();
        if (src.ok() && !Expression::isInt(src.value())) {
            status = Status::Error("SRC Vertex ID should be of type integer");
            break;
        }
        edge_.set_src(Expression::asInt(src.value()));

        status = sentence_->getDstId()->prepare();
        if (!status.ok()) {
            break;
        }
        auto dst = sentence_->getDstId()->eval();
        if (dst.ok() && !Expression::isInt(dst.value())) {
            status = Status::Error("DST Vertex ID should be of type integer");
            break;
        }
        edge_.set_dst(Expression::asInt(dst.value()));
        edge_.set_ranking(sentence_->getRank());

        auto spaceId = ectx()->rctx()->session()->space();
        edgeTypeName_ = sentence_->getEdgeType();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *edgeTypeName_);
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        edgeType_ = edgeStatus.value();
        schema_ = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType_);
        if (schema_ == nullptr) {
            status = Status::Error("No schema found for '%s'", edgeTypeName_->c_str());
            break;
        }
        edge_.set_edge_type(edgeType_);

        status = prepareSet();
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
    } while (false);

    return status;
}


Status UpdateEdgeExecutor::prepareSet() {
    auto status = Status::OK();
    auto items = sentence_->updateList()->items();
    for (auto& item : items) {
        // item: name_label ASSIGN expression
        storage::cpp2::UpdateItem updateItem;
        updateItem.name = *edgeTypeName_;
        auto propName = item->field();
        if (schema_->field(*propName) == nullptr) {
            status = Status::Error("The schema '%s' does not contain the property '%s'",
                                   edgeTypeName_->c_str(), propName->c_str());
            break;
        }
        updateItem.field = *propName;
        item->value()->setContext(expCtx_.get());
        status = item->value()->prepare();
        if (!status.ok()) {
            status = Status::Error("Invalid update item value expression: "
                                   + item->toString());
            break;
        }
        updateItem.value = Expression::encode(item->value());
        updateItems_.emplace_back(std::move(updateItem));
    }
    if (updateItems_.empty()) {
        status = Status::Error("There must be some correct update items");
    }
    return status;
}


Status UpdateEdgeExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    return Status::OK();
}


Status UpdateEdgeExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        yields_ = clause->columns();
    }
    return Status::OK();
}


Status UpdateEdgeExecutor::prepareNeededProps() {
    auto status = Status::OK();
    do {
        if (filter_ != nullptr) {
            filter_->setContext(expCtx_.get());
            status = filter_->prepare();
            if (!status.ok()) {
                status = Status::Error("Invalid where clause: " + filter_->toString());
                break;
            }
        }
        if (yields_.empty()) {
            break;
        }
        for (auto *col : yields_) {
            col->expr()->setContext(yieldExpCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }
    } while (false);
    return status;
}


StatusOr<std::vector<storage::cpp2::PropDef>> UpdateEdgeExecutor::getDstProps() {
    std::unordered_map<std::string, std::vector<std::string>> tagProps;
    for (auto &tagProp : expCtx_->dstTagProps()) {
        tagProps[tagProp.first].emplace_back(tagProp.second);
    }

    auto spaceId = ectx()->rctx()->session()->space();
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


StatusOr<std::vector<storage::cpp2::PropDef>> UpdateEdgeExecutor::getReturnProps() {
    std::vector<storage::cpp2::PropDef> props;
    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &tagProp : yieldExpCtx_->srcTagProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = tagProp.second;
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagProp.first);
        }
        auto tagId = status.value();
        pd.set_tag_id(tagId);
        props.emplace_back(std::move(pd));
        schemaPropIndex_.emplace(std::make_pair(tagProp.first, tagProp.second),
                                 props.size() - 1);
    }
    for (auto &edgeProp : yieldExpCtx_->aliasProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::EDGE;
        pd.name = edgeProp.second;
        props.emplace_back(std::move(pd));
        schemaPropIndex_.emplace(std::make_pair(*edgeTypeName_, edgeProp.second),
                                 props.size() - 1);
    }
    return props;
}


bool UpdateEdgeExecutor::insertReverselyEdge() {
    std::vector<storage::cpp2::Edge> edges;
    storage::cpp2::Edge reverselyEdge;
    reverselyEdge.key.set_src(edge_.dst);
    reverselyEdge.key.set_dst(edge_.src);
    reverselyEdge.key.set_ranking(edge_.ranking);
    reverselyEdge.key.set_edge_type(-edgeType_);
    reverselyEdge.props = "";
    edges.emplace_back(reverselyEdge);
    bool ret = true;
    folly::Baton<true, std::atomic> baton;
    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->addEdges(space, std::move(edges), false);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [&, this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            ret = false;
        }
        baton.post();
    };
    auto error = [&, this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        ret = false;
        baton.post();
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
    baton.wait();
    return ret;
}


void UpdateEdgeExecutor::finishExecution(RpcUpdateResponse &&rpcResp) {
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    std::vector<std::string> columnNames;
    columnNames.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            columnNames.emplace_back(col->expr()->toString());
        } else {
            columnNames.emplace_back(*col->alias());
        }
    }
    resp_->set_column_names(std::move(columnNames));

    std::vector<cpp2::RowValue> rows;
    for (auto& resp : rpcResp.responses()) {
        if (resp.return_data.empty()) {
            continue;
        }
        std::shared_ptr<ResultSchemaProvider> schema;
        if (resp.get_schema() != nullptr) {
            schema = std::make_shared<ResultSchemaProvider>(resp.schema);
        }
        for (auto& vp : resp.return_data) {
            if (insertable_ && vp.get_upsert()) {
                // TODO If this executor will support for updating multiple edges
                // in the future, it should collect all the vp.edge_key, then insert
                // them reversely edge at once before return.
                if (!insertReverselyEdge()) {
                    // Very bad, it should delete the upsert positive edge!!!
                    DCHECK(onError_);
                    onError_(Status::Error("Internal error about insert reversely edge."));
                    return;
                }
            }
            auto reader = RowReader::getRowReader(vp.data, schema);
            auto &getters = expCtx_->getters();
            getters.getAliasProp = [&, this] (const std::string &,
                                              const std::string &prop) -> OptVariantType {
                std::string edgeName = *this->edgeTypeName_;
                auto indexIt = this->schemaPropIndex_.find(std::make_pair(edgeName, prop));
                if (indexIt == this->schemaPropIndex_.end()) {
                    auto msg = folly::sformat(
                        "EdgeName : {} , propName : {} is not exist", edgeName, prop);
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
                }
                auto index = indexIt->second;
                const nebula::cpp2::ValueType &type = schema->getFieldType(index);
                if (type == CommonConstants::kInvalidValueType()) {
                    auto msg =
                        folly::sformat("Edge: {} no schema for the index {}", edgeName, index);
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
                }
                auto res = RowReader::getPropByIndex(reader.get(), index);
                if (ok(res)) {
                    return value(std::move(res));
                }
                return Status::Error(folly::sformat("{}.{} was not exist", edgeName, prop));
            };
            getters.getSrcTagProp = [&, this] (const std::string &tagName,
                                               const std::string &prop) -> OptVariantType {
                auto tagIt = this->schemaPropIndex_.find(std::make_pair(tagName, prop));
                if (tagIt == this->schemaPropIndex_.end()) {
                    auto msg = folly::sformat(
                        "Src tagName : {} , propName : {} is not exist", tagName, prop);
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
                }
                auto index = tagIt->second;
                const nebula::cpp2::ValueType &type = schema->getFieldType(index);
                if (type == CommonConstants::kInvalidValueType()) {
                    auto msg =
                        folly::sformat("Tag: {} no schema for the index {}", tagName, index);
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
                }
                auto res = RowReader::getPropByIndex(reader.get(), index);
                if (ok(res)) {
                    return value(std::move(res));
                }
                return Status::Error(folly::sformat("{}.{} was not exist", tagName, prop));
            };
            getters.getDstTagProp = [&, this] (const std::string &tagName,
                                               const std::string &prop) -> OptVariantType {
                auto tagIt = this->dstTagProps_.find(std::make_pair(tagName, prop));
                if (tagIt == this->dstTagProps_.end()) {
                    auto msg = folly::sformat(
                        "Dst tagName : {} , propName : {} is not exist", tagName, prop);
                    LOG(ERROR) << msg;
                    return Status::Error(msg);
                }
                return vertexHolder_->get(this->edge_.dst, tagIt->second);
            };

            std::vector<VariantType> record;
            record.reserve(yields_.size());
            for (auto *column : yields_) {
                auto *expr = column->expr();
                auto value = expr->eval();
                if (!value.ok()) {
                    onError_(value.status());
                    return;
                }
                record.emplace_back(std::move(value.value()));
            }
            std::vector<cpp2::ColumnValue> row(yields_.size());
            auto index = 0;
            for (auto &column : record) {
                switch (column.which()) {
                    case VAR_INT64:
                        row[index].set_integer(boost::get<int64_t>(column));
                        break;
                    case VAR_DOUBLE:
                        row[index].set_double_precision(boost::get<double>(column));
                        break;
                    case VAR_BOOL:
                        row[index].set_bool_val(boost::get<bool>(column));
                        break;
                    case VAR_STR:
                        row[index].set_str(boost::get<std::string>(column));
                        break;
                    default:
                        LOG(FATAL) << "Unknown VariantType: " << column.which();
                }
                index++;
            }
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
    }
    resp_->set_rows(std::move(rows));
    DCHECK(onFinish_);
    onFinish_();
}


void UpdateEdgeExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}


bool UpdateEdgeExecutor::fetchDstVertexProps() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto statusOr = getDstProps();
    if (!statusOr.ok()) {
        LOG(ERROR) << "Internal error about getDstProps.";
        return false;
    }
    auto returns = statusOr.value();
    if (returns.empty()) {
        return false;
    }
    folly::Baton<true, std::atomic> baton;
    std::vector<VertexID> vertices;
    vertices.emplace_back(edge_.get_dst());
    auto future = ectx()->storage()->getVertexProps(spaceId,
                                                    std::move(vertices),
                                                    std::move(returns));
    auto *runner = ectx()->rctx()->runner();
    bool ret = true;
    auto cb = [&, this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness != 100) {
            ret = false;
            baton.post();
            return;
        }
        if (this->vertexHolder_ == nullptr) {
            this->vertexHolder_ = std::make_unique<VertexHolder>();
        }
        for (auto& resp : result.responses()) {
            this->vertexHolder_->add(resp);
        }
        baton.post();
    };
    auto error = [&, this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        ret = false;
        baton.post();
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
    baton.wait();
    return ret;
}


bool UpdateEdgeExecutor::applyDestPropExp(const Expression* exp) {
    switch (exp->kind()) {
        case Expression::kDestProp: {
            auto* destPropExp = static_cast<const DestPropertyExpression*>(exp);
            destPropExp->setPreEval();
            return true;
        }
        case Expression::kPrimary:
        case Expression::kSourceProp:
        case Expression::kEdgeRank:
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId:
        case Expression::kEdgeType:
        case Expression::kAliasProp:
        case Expression::kVariableProp:
        case Expression::kInputProp:
            return true;
        case Expression::kFunctionCall:
            return false;
        case Expression::kUnary: {
            auto* unaExp = static_cast<const UnaryExpression*>(exp);
            return applyDestPropExp(unaExp->operand());
        }
        case Expression::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return applyDestPropExp(typExp->operand());
        }
        case Expression::kArithmetic: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            return applyDestPropExp(ariExp->left()) && applyDestPropExp(ariExp->right());
        }
        case Expression::kRelational: {
            auto* relExp = static_cast<const RelationalExpression*>(exp);
            return applyDestPropExp(relExp->left()) && applyDestPropExp(relExp->right());
        }
        case Expression::kLogical: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            return applyDestPropExp(logExp->left()) && applyDestPropExp(logExp->right());
        }
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return false;
        }
    }
}


void UpdateEdgeExecutor::execute() {
    FLOG_INFO("Executing UpdateEdge: %s", sentence_->toString().c_str());
    if (expCtx_->hasDstTagProp()) {
        if (!fetchDstVertexProps()) {
            DCHECK(onError_);
            onError_(Status::Error("Internal error when fetch dst vertex props."));
            return;
        }
        auto &getters = this->expCtx_->getters();
        getters.getDstTagProp = [&, this] (const std::string &tagName,
                                           const std::string &prop) -> OptVariantType {
            auto tagIt = this->dstTagProps_.find(std::make_pair(tagName, prop));
            if (tagIt == this->dstTagProps_.end()) {
                auto msg = folly::sformat(
                    "Dst tagName : {} , propName : {} is not exist", tagName, prop);
                LOG(ERROR) << msg;
                return Status::Error(msg);
            }
            return vertexHolder_->get(this->edge_.dst, tagIt->second);
        };
        // Update all expressions in Set and Where clauses
        if (filter_ != nullptr) {
            if (!applyDestPropExp(filter_)) {
                DCHECK(onError_);
                onError_(Status::Error("Internal error: apply DestPropertyExpression!"));
                return;
            }
        }
        auto index = 0;
        for (auto& item : sentence_->updateList()->items()) {
            auto valueExp = item->value();
            if (!applyDestPropExp(valueExp)) {
                DCHECK(onError_);
                onError_(Status::Error("Internal error: apply DestPropertyExpression!"));
                return;
            }
            updateItems_[index].value = Expression::encode(valueExp);
            index++;
        }
    }

    auto space = ectx()->rctx()->session()->space();
    std::vector<storage::cpp2::EdgeKey> edges;
    edges.emplace_back(edge_);
    std::string filterStr = filter_ ? Expression::encode(filter_) : "";
    auto statusOr = this->getReturnProps();
    if (!statusOr.ok()) {
        onError_(std::move(statusOr.status()));
        return;
    }
    auto returns = statusOr.value();
    auto future = ectx()->storage()->updateEdge(space,
                                                std::move(edges),
                                                edgeType_,
                                                filterStr,
                                                std::move(updateItems_),
                                                std::move(returns),
                                                insertable_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error about updateEdge"));
            return;
        }
        this->finishExecution(std::move(resp));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error about updateEdge"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


OptVariantType UpdateEdgeExecutor::VertexHolder::get(const VertexID id,
                                                  const int64_t index) const {
    auto iter = data_.find(id);
    if (iter == data_.end()) {
        return Status::Error("vertex was not found %ld", id);
    }

    auto reader = RowReader::getRowReader(iter->second, schema_);
    auto res = RowReader::getPropByIndex(reader.get(), index);
    if (!ok(res)) {
        return Status::Error("get prop failed");
    }
    return value(std::move(res));
}


void UpdateEdgeExecutor::VertexHolder::add(const storage::cpp2::QueryResponse &resp) {
    auto *vertices = resp.get_vertices();
    if (vertices == nullptr) {
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
        data_[vdata.vertex_id] = vdata.vertex_data;
    }
}

}   // namespace graph
}   // namespace nebula
