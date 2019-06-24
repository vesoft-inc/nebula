/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AlterEdgeExecutor.h"

namespace nebula {
namespace graph {

AlterEdgeExecutor::AlterEdgeExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterEdgeSentence*>(sentence);
}


Status AlterEdgeExecutor::prepare() {
    auto status = checkIfGraphSpaceChosen();

    if (!status.ok()) {
        return status;
    }

    const auto& schemaOpts = sentence_->getSchemaOpts();

    for (auto& schemaOpt : schemaOpts) {
        nebula::meta::cpp2::AlterSchemaItem schemaItem;
        auto opType = schemaOpt->toType();
        schemaItem.set_op(std::move(opType));
        const auto& specs = schemaOpt->columnSpecs();
        nebula::cpp2::Schema schema;
        for (auto& spec : specs) {
            nebula::cpp2::ColumnDef column;
            column.name = *spec->name();
            column.type.type = columnTypeToSupportedType(spec->type());
            schema.columns.emplace_back(std::move(column));
        }
        schemaItem.set_schema(std::move(schema));
        options_.emplace_back(std::move(schemaItem));
    }

    const auto& schemaProps = sentence_->getSchemaProps();

    for (auto& schemaProp : schemaProps) {
        auto propType = schemaProp->getPropType();
        StatusOr<int64_t> retInt;
        StatusOr<std::string> retStr;
        int ttlDuration;
        switch (propType) {
            case SchemaPropItem::TTL_DURATION:
                retInt = schemaProp->getTtlDuration();
                if (!retInt.ok()) {
                   return retInt.status();
                }
                ttlDuration = retInt.value();
                if (ttlDuration < 0) {
                    ttlDuration = 0;
                }
                schemaProp_.set_ttl_duration(ttlDuration);
                break;
            case SchemaPropItem::TTL_COL:
                // Check the legality of the column in meta
                retStr = schemaProp->getTtlCol();
                if (!retStr.ok()) {
                   return retStr.status();
                }
                schemaProp_.set_ttl_col(retStr.value());
                break;
            default:
                return Status::Error("Property type not support");
        }
    }

    return Status::OK();
}


void AlterEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->alterEdgeSchema(spaceId, *name, std::move(options_), std::move(schemaProp_));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
