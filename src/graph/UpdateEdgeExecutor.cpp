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
                                       ExecutionContext *ectx)
    : Executor(ectx, "update_edge") {
    sentence_ = static_cast<UpdateEdgeSentence*>(sentence);
}

Status UpdateEdgeExecutor::prepare() {
    return Status::OK();
}

Status UpdateEdgeExecutor::prepareData() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();

    spaceId_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(spaceId_);
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setOnVariableVariantGet(onVariableVariantGet_);
    Getters getters;

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        insertable_ = sentence_->getInsertable();
        auto sid = sentence_->getSrcId();
        sid->setContext(expCtx_.get());
        status = sid->prepare();
        if (!status.ok()) {
            break;
        }
        auto src = sid->eval(getters);
        if (!src.ok() || !Expression::isInt(src.value())) {
            status = Status::Error("SRC Vertex ID should be of type integer");
            break;
        }
        edge_.set_src(Expression::asInt(src.value()));

        auto did = sentence_->getDstId();
        did->setContext(expCtx_.get());
        status = did->prepare();
        if (!status.ok()) {
            break;
        }
        auto dst = did->eval(getters);
        if (!dst.ok() || !Expression::isInt(dst.value())) {
            status = Status::Error("DST Vertex ID should be of type integer");
            break;
        }
        edge_.set_dst(Expression::asInt(dst.value()));
        edge_.set_ranking(sentence_->getRank());

        edgeTypeName_ = sentence_->getEdgeType();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, *edgeTypeName_);
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

    if (!status.ok()) {
        stats::Stats::addStatsValue(stats_.get(), false, duration().elapsedInUSec());
    }
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


void UpdateEdgeExecutor::toResponse(storage::cpp2::UpdateResponse &&rpcResp) {
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
                        LOG(ERROR) << "Unknown VariantType: " << column.which();
                        doError(Status::Error("Unknown VariantType: %d", column.which()));
                        return;
                }
            } else {
                doError(Status::Error("get property failed"));
                return;
            }
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
    }
    resp_->set_rows(std::move(rows));
}

void UpdateEdgeExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void UpdateEdgeExecutor::updateEdge(bool reversely) {
    std::string filterStr = "";
    std::vector<std::string> returns;
    if (reversely) {
        auto src = edge_.src;
        auto dst = edge_.dst;
        edge_.set_src(dst);
        edge_.set_dst(src);
        edge_.set_edge_type(-edge_.edge_type);
    } else {
        filterStr = filter_ ? Expression::encode(filter_) : "";
        returns = getReturnColumns();
    }
    auto future = ectx()->getStorageClient()->updateEdge(spaceId_,
                                                         edge_,
                                                         std::move(filterStr),
                                                         updateItems_,
                                                         std::move(returns),
                                                         insertable_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, reversely] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Update edge(%s) `%ld->%ld@%ld' failed: %s",
                        edgeTypeName_->c_str(),
                        edge_.src, edge_.dst, edge_.ranking,
                        resp.status().toString().c_str()));
            return;
        }
        auto rpcResp = std::move(resp).value();
        for (auto& code : rpcResp.get_result().get_failed_codes()) {
            switch (code.get_code()) {
                case nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER:
                    doError(Status::Error("Maybe invalid edge or property in WHEN clause!"));
                    return;
                case nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER:
                    doError(Status::Error("Maybe invalid property in SET/YIELD clause!"));
                    return;
                case nebula::storage::cpp2::ErrorCode::E_FILTER_OUT:
                    // Return ok when filter out without exception
                    // so do nothing
                    // https://github.com/vesoft-inc/nebula/issues/1888
                    // TODO(shylock) maybe we need alert user execute ok but no data affect
                    this->toResponse(std::move(rpcResp));
                    doFinish(Executor::ProcessControl::kNext);
                    return;
                default:
                    std::string errMsg =
                        folly::stringPrintf("Maybe edge does not exist, "
                                            "part: %d, error code: %d!",
                                            code.get_part_id(),
                                            static_cast<int32_t>(code.get_code()));
                    LOG(ERROR) << errMsg;
                    doError(Status::Error(errMsg));
                    return;
            }
        }
        if (reversely) {
            doFinish(Executor::ProcessControl::kNext);
        } else {
            this->toResponse(std::move(rpcResp));
            this->updateEdge(true);
        }
    };
    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Update edge(%s) `%ld->%ld@%ld'  exception: %s",
                        edgeTypeName_->c_str(),
                        edge_.src, edge_.dst, edge_.ranking,
                        e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void UpdateEdgeExecutor::execute() {
    auto status = prepareData();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    updateEdge(false);
}

}   // namespace graph
}   // namespace nebula
