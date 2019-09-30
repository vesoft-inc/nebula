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

// UPDATE/UPSERT EDGE <vertex_id> -> <vertex_id> [@<ranking>] OF <edge_type>
// SET <update_decl> [WHEN <conditions>] [YIELD <field_list>]
UpdateEdgeExecutor::UpdateEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UpdateEdgeSentence*>(sentence);
}


Status UpdateEdgeExecutor::prepare() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();
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
        if (!src.ok() || !Expression::isInt(src.value())) {
            status = Status::Error("SRC Vertex ID should be of type integer");
            break;
        }
        edge_.set_src(Expression::asInt(src.value()));

        status = sentence_->getDstId()->prepare();
        if (!status.ok()) {
            break;
        }
        auto dst = sentence_->getDstId()->eval();
        if (!dst.ok() || !Expression::isInt(dst.value())) {
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
        auto edgeType = edgeStatus.value();
        edge_.set_edge_type(edgeType);

        status = prepareSet();
        if (!status.ok()) {
            break;
        }
        status = prepareWhen();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
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
        updateItem.prop = *propName;
        updateItem.value = Expression::encode(item->value());
        updateItems_.emplace_back(std::move(updateItem));
    }
    if (updateItems_.empty()) {
        status = Status::Error("There must be some correct update items.");
    }
    return status;
}


Status UpdateEdgeExecutor::prepareWhen() {
    auto *clause = sentence_->whenClause();
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


std::vector<std::string> UpdateEdgeExecutor::getReturnColumns() {
    std::vector<std::string> returnColumns;
    for (auto *col : yields_) {
        auto column = Expression::encode(col->expr());
        returnColumns.emplace_back(std::move(column));
    }
    return returnColumns;
}


void UpdateEdgeExecutor::finishExecution(storage::cpp2::UpdateResponse &&rpcResp) {
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
    if (rpcResp.__isset.schema && rpcResp.__isset.data) {
        auto schema = std::make_shared<ResultSchemaProvider>(rpcResp.schema);
        auto reader = RowReader::getRowReader(rpcResp.data, schema);
        std::vector<cpp2::ColumnValue> row(yields_.size());
        for (auto index = 0UL; index < yields_.size(); index++) {
            auto res = RowReader::getPropByIndex(reader.get(), index);
            if (ok(res)) {
                auto column = value(std::move(res));
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
            } else {
                DCHECK(onError_);
                onError_(Status::Error("get property failed"));
                return;
            }
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
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


void UpdateEdgeExecutor::insertReverselyEdge(storage::cpp2::UpdateResponse &&rpcResp) {
    std::vector<storage::cpp2::Edge> edges;
    storage::cpp2::Edge reverselyEdge;
    reverselyEdge.key.set_src(edge_.dst);
    reverselyEdge.key.set_dst(edge_.src);
    reverselyEdge.key.set_ranking(edge_.ranking);
    reverselyEdge.key.set_edge_type(-edge_.edge_type);
    reverselyEdge.props = "";
    edges.emplace_back(reverselyEdge);
    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->getStorageClient()->addEdges(space, std::move(edges), false);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, updateResp = std::move(rpcResp)] (auto &&resp) mutable {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            // Very bad, it should delete the upsert positive edge!!!
            DCHECK(onError_);
            onError_(Status::Error("Insert the reversely edge failed."));
            return;
        }
        this->finishExecution(std::move(updateResp));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        // Very bad, it should delete the upsert positive edge!!!
        DCHECK(onError_);
        onError_(Status::Error("Internal error: insert reversely edge."));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void UpdateEdgeExecutor::execute() {
    FLOG_INFO("Executing UpdateEdge: %s", sentence_->toString().c_str());
    auto space = ectx()->rctx()->session()->space();
    std::string filterStr = filter_ ? Expression::encode(filter_) : "";
    auto returns = getReturnColumns();
    auto future = ectx()->getStorageClient()->updateEdge(space,
                                                         edge_,
                                                         filterStr,
                                                         std::move(updateItems_),
                                                         std::move(returns),
                                                         insertable_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto rpcResp = std::move(resp).value();
        if (insertable_ && rpcResp.get_upsert()) {
            // TODO(zhangguoqing) Making the reverse edge of insertion is transactional
            this->insertReverselyEdge(std::move(rpcResp));
        } else {
            this->finishExecution(std::move(rpcResp));
        }
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error about updateEdge"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
