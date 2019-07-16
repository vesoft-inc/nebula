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

// UPDATE [OR INSERT] VERTEX <vertex_id>
// SET <update_decl> [WHERE <conditions>] [YIELD <field_list>]
UpdateVertexExecutor::UpdateVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UpdateVertexSentence*>(sentence);
}


Status UpdateVertexExecutor::prepare() {
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
        status = sentence_->getVid()->prepare();
        if (!status.ok()) {
            break;
        }
        auto vid = sentence_->getVid()->eval();
        if (vid.ok() && !Expression::isInt(vid.value())) {
            status = Status::Error("Get Vertex ID failure!");
            break;
        }
        vertex_ = Expression::asInt(vid.value());
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


Status UpdateVertexExecutor::prepareSet() {
    auto status = Status::OK();
    auto items = sentence_->updateList()->items();
    for (auto& item : items) {
        // item: name_label ASSIGN expression, name_label is alias_ref_expression
        storage::cpp2::UpdateItem updateItem;
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(*item->field());
        if (!expRet.ok()) {
            status = Status::Error("Invalid vertex update item field: " + *item->field());
            break;
        }
        auto expr = std::move(expRet).value();
        // alias_ref_expression(LABLE DOT LABLE): TagName.PropName
        auto* eexpr = static_cast<const AliasPropertyExpression*>(expr.get());
        updateItem.name = *eexpr->alias();
        updateItem.field = *eexpr->prop();
        // check the tag scheam and the prop in it?
        item->value()->setContext(expCtx_.get());
        status = item->value()->prepare();
        if (!status.ok()) {
            status = Status::Error("Invalid update item value expression: "
                                   + item->value()->toString());
            break;
        }
        updateItem.value = Expression::encode(item->value());
        updateItems_.emplace_back(std::move(updateItem));
    }
    if (updateItems_.empty()) {
        status = Status::Error("There must be some correct update items!");
    }
    return status;
}


Status UpdateVertexExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
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


Status UpdateVertexExecutor::prepareNeededProps() {
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
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (expCtx_->hasDstTagProp() || expCtx_->hasEdgeProp()) {
        status = Status::SyntaxError("Sentence maybe contain dest property or "
                                     "edge property expression.");
    }
    return status;
}


StatusOr<std::vector<storage::cpp2::PropDef>> UpdateVertexExecutor::getReturnProps() {
    std::vector<storage::cpp2::PropDef> props;
    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &tagProp : yieldExpCtx_->srcTagProps()) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::SOURCE;
        pd.name = tagProp.second;
        auto tagStatus = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
        if (!tagStatus.ok()) {
            return Status::Error("No schema found for %s", tagProp.first);
        }
        auto tagId = tagStatus.value();
        pd.set_tag_id(tagId);
        props.emplace_back(std::move(pd));
        schemaPropIndex_.emplace(std::make_pair(tagProp.first, tagProp.second),
                                 props.size() - 1);
    }
    return props;
}


void UpdateVertexExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}


void UpdateVertexExecutor::finishExecution(RpcResponse &&rpcResp) {
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
    for (auto &resp : rpcResp.responses()) {
        if (resp.return_data.empty()) {
            continue;
        }
        std::shared_ptr<ResultSchemaProvider> schema;
        if (resp.get_schema() != nullptr) {
            schema = std::make_shared<ResultSchemaProvider>(resp.schema);
        }
        for (auto& vp : resp.return_data) {
            auto reader = RowReader::getRowReader(vp.data, schema);
            auto &getters = expCtx_->getters();
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


void UpdateVertexExecutor::execute() {
    FLOG_INFO("Executing UpdateVertex: %s", sentence_->toString().c_str());
    auto spaceId = ectx()->rctx()->session()->space();
    std::vector<VertexID> vertices;
    vertices.emplace_back(vertex_);
    std::string filterStr = filter_ ? Expression::encode(filter_) : "";
    auto statusOr = this->getReturnProps();
    if (!statusOr.ok()) {
        onError_(std::move(statusOr.status()));
        return;
    }
    auto returns = statusOr.value();
    auto future = ectx()->storage()->updateVertex(spaceId,
                                                  std::move(vertices),
                                                  filterStr,
                                                  std::move(updateItems_),
                                                  std::move(returns),
                                                  insertable_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
        }
        this->finishExecution(std::move(resp));
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
