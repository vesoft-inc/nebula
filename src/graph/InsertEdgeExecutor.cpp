/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InsertEdgeExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

InsertEdgeExecutor::InsertEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertEdgeSentence*>(sentence);
}


Status InsertEdgeExecutor::prepare() {
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    return Status::OK();
}


Status InsertEdgeExecutor::check() {
    Status status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        LOG(ERROR) << "Please choose the space before insert edge";
        return status;
    }

    spaceId_ = ectx()->rctx()->session()->space();
    overwritable_ = sentence_->overwritable();
    auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, *sentence_->edge());
    if (!edgeStatus.ok()) {
        status = edgeStatus.status();
        return status;
    }
    edgeType_ = edgeStatus.value();
    props_ = sentence_->properties();
    rows_ = sentence_->rows();

    expCtx_->setStorageClient(ectx()->getStorageClient());
    schema_ = ectx()->schemaManager()->getEdgeSchema(spaceId_, edgeType_);
    if (schema_ == nullptr) {
        LOG(ERROR) << "No schema found for " << sentence_->edge();
        return Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
    }

    if (props_.size() > schema_->getNumFields()) {
        LOG(ERROR) << "Input props number " << props_.size()
                   << ", schema fields number " << schema_->getNumFields();
        return Status::Error("Wrong number of props");
    }

    auto *mc = ectx()->getMetaClient();

    for (size_t i = 0; i < schema_->getNumFields(); i++) {
        std::string name = schema_->getFieldName(i);
        auto it = std::find_if(props_.begin(), props_.end(),
                               [name](std::string *prop) { return *prop == name;});

        if (it == props_.end()) {
            auto valueResult = mc->getEdgeDefaultValue(spaceId_, edgeType_, name).get();

            if (!valueResult.ok()) {
                LOG(ERROR) << "Not exist default value: " << name;
                return Status::Error("Not exist default value");
            } else {
                VLOG(3) << "Default Value: " << name << " : " << valueResult.value();
                defaultValues_.emplace(name, valueResult.value());
            }
        } else {
            int index = std::distance(props_.begin(), it);
            propsPosition_.emplace(name, index);
        }
    }

    return Status::OK();
}


StatusOr<std::vector<storage::cpp2::Edge>> InsertEdgeExecutor::prepareEdges() {
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());

    auto space = ectx()->rctx()->session()->space();
    expCtx_->setSpace(space);

    std::vector<storage::cpp2::Edge> edges(rows_.size() * 2);   // inbound and outbound
    auto index = 0;
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto sid = row->srcid();
        sid->setContext(expCtx_.get());
        auto status = sid->prepare();
        if (!status.ok()) {
            return status;
        }
        auto ovalue = sid->eval();
        if (!ovalue.ok()) {
            return ovalue.status();
        }

        auto v = ovalue.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto src = Expression::asInt(v);

        auto did = row->dstid();
        did->setContext(expCtx_.get());
        status = did->prepare();
        if (!status.ok()) {
            return status;
        }
        ovalue = did->eval();
        if (!ovalue.ok()) {
            return ovalue.status();
        }

        v = ovalue.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        auto dst = Expression::asInt(v);

        int64_t rank = row->rank();

        auto expressions = row->values();

        std::vector<VariantType> values;
        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            status = expr->prepare();
            if (!status.ok()) {
                return status;
            }

            ovalue = expr->eval();
            if (!ovalue.ok()) {
                return ovalue.status();
            }
            values.emplace_back(ovalue.value());
        }

        RowWriter writer(schema_);
        for (size_t schemaIndex = 0; schemaIndex < schema_->getNumFields(); schemaIndex++) {
            auto fieldName = schema_->getFieldName(schemaIndex);
            auto positionIter = propsPosition_.find(fieldName);

            VariantType value;
            auto schemaType = schema_->getFieldType(schemaIndex);
            if (positionIter != propsPosition_.end()) {
                auto position = propsPosition_[fieldName];
                value = values[position];
                if (!checkValueType(schemaType, value)) {
                   DCHECK(onError_);
                    LOG(ERROR) << "ValueType is wrong, schema type "
                               << static_cast<int32_t>(schemaType.type)
                                << ", input type " <<  value.which();
                    return Status::Error("ValueType is wrong");
                }
            } else {
                // fetch default value from cache
                auto result = transformDefaultValue(schemaType.type, defaultValues_[fieldName]);
                if (!result.ok()) {
                    return result.status();
                }

                value = result.value();
                VLOG(3) << "Supplement default value : " << fieldName << " : " << value;
            }

            if (schemaType.type == nebula::cpp2::SupportedType::TIMESTAMP) {
                auto timestamp = toTimestamp(value);
                if (!timestamp.ok()) {
                    return timestamp.status();
                }
                writeVariantType(writer, timestamp.value());
            } else {
                writeVariantType(writer, value);
            }
        }

        {
            auto &out = edges[index++];
            out.key.set_src(src);
            out.key.set_dst(dst);
            out.key.set_ranking(rank);
            out.key.set_edge_type(edgeType_);
            out.props = writer.encode();
            out.__isset.key = true;
            out.__isset.props = true;
        }
        {
            auto &in = edges[index++];
            in.key.set_src(dst);
            in.key.set_dst(src);
            in.key.set_ranking(rank);
            in.key.set_edge_type(-edgeType_);
            in.props = "";
            in.__isset.key = true;
            in.__isset.props = true;
        }
    }

    return edges;
}


void InsertEdgeExecutor::execute() {
    auto status = check();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getInsertEdgeStats());
        return;
    }

    auto result = prepareEdges();
    if (!result.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getInsertEdgeStats());
        return;
    }

    auto future = ectx()->getStorageClient()->addEdges(spaceId_,
                                                       std::move(result).value(),
                                                       overwritable_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        // For insertion, we regard partial success as failure.
        auto completeness = resp.completeness();
        if (completeness != 100) {
            doError(Status::Error("Internal Error"), ectx()->getGraphStats()->getInsertEdgeStats());
            return;
        }
        doFinish(Executor::ProcessControl::kNext,
                 ectx()->getGraphStats()->getInsertEdgeStats(),
                 rows_.size());
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"), ectx()->getGraphStats()->getInsertEdgeStats());
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula


