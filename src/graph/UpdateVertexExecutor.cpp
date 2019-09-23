/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "filter/Expressions.h"
#include "graph/UpdateVertexExecutor.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace graph {

// UPDATE/UPSERT VERTEX <vertex_id>
// SET <update_decl> [WHEN <conditions>] [YIELD <field_list>]
UpdateVertexExecutor::UpdateVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UpdateVertexSentence*>(sentence);
}


Status UpdateVertexExecutor::prepare() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        insertable_ = sentence_->getInsertable();
        status = sentence_->getVid()->prepare();
        if (!status.ok()) {
            break;
        }
        auto vid = sentence_->getVid()->eval();
        if (!vid.ok() || !Expression::isInt(vid.value())) {
            status = Status::Error("Get Vertex ID failure!");
            break;
        }
        vertex_ = Expression::asInt(vid.value());
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


Status UpdateVertexExecutor::prepareSet() {
    auto status = Status::OK();
    auto items = sentence_->updateList()->items();
    for (auto& item : items) {
        // item: name_label ASSIGN expression, name_label is alias_ref_expression
        storage::cpp2::UpdateItem updateItem;
        auto expRet = Expression::decode(*item->field());
        if (!expRet.ok()) {
            status = Status::Error("Invalid vertex update item field: " + *item->field());
            break;
        }
        auto expr = std::move(expRet).value();
        // alias_ref_expression(LABLE DOT LABLE): TagName.PropName
        auto* eexpr = static_cast<const AliasPropertyExpression*>(expr.get());
        updateItem.name = *eexpr->alias();
        updateItem.prop = *eexpr->prop();
        updateItem.value = Expression::encode(item->value());
        updateItems_.emplace_back(std::move(updateItem));
    }
    if (updateItems_.empty()) {
        status = Status::Error("There must be some correct update items!");
    }
    return status;
}


Status UpdateVertexExecutor::prepareWhen() {
    auto *clause = sentence_->whenClause();
    if (clause != nullptr) {
        filter_ = clause->filter();
    }
    return Status::OK();
}


Status UpdateVertexExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        yields_ = clause->columns();
    }
    return Status::OK();
}


std::vector<std::string> UpdateVertexExecutor::getReturnColumns() {
    std::vector<std::string> returnColumns;
    for (auto *col : yields_) {
        auto column = Expression::encode(col->expr());
        returnColumns.emplace_back(std::move(column));
    }
    return returnColumns;
}


void UpdateVertexExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}


void UpdateVertexExecutor::finishExecution(storage::cpp2::UpdateResponse &&rpcResp) {
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


void UpdateVertexExecutor::execute() {
    FLOG_INFO("Executing UpdateVertex: %s", sentence_->toString().c_str());
    auto spaceId = ectx()->rctx()->session()->space();
    std::string filterStr = filter_ ? Expression::encode(filter_) : "";
    auto returns = getReturnColumns();
    auto future = ectx()->storage()->updateVertex(spaceId,
                                                  vertex_,
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
        this->finishExecution(std::move(rpcResp));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
