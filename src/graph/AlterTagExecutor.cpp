/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AlterTagExecutor.h"

namespace nebula {
namespace graph {

AlterTagExecutor::AlterTagExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterTagSentence*>(sentence);
}


Status AlterTagExecutor::prepare() {
    auto status = checkIfGraphSpaceChosen();

    if (!status.ok()) {
        return status;
    }

    const auto& schemaOpts = sentence_->getSchemaOpts();

    for (auto& schemaOpt : schemaOpts) {
        nebula::meta::cpp2::AlterSchemaOption schemaOption;
        auto opType = schemaOpt->toOptionType();
        schemaOption.set_type(std::move(opType));
        const auto& specs = schemaOpt->columnSpecs();
        nebula::cpp2::Schema schema;
        for (auto& spec : specs) {
            nebula::cpp2::ColumnDef column;
            column.name = *spec->name();
            column.type.type = columnTypeToSupportedType(spec->type());
            schema.columns.emplace_back(std::move(column));
        }
        schemaOption.set_schema(std::move(schema));
        options_.emplace_back(std::move(schemaOption));
    }

    const auto& schemaProps = sentence_->getSchemaProps();

    for (auto& schemaProp : schemaProps) {
        nebula::meta::cpp2::AlterSchemaProp alterSchemaProp;
        auto propType = schemaProp->getPropType();
        StatusOr<int64_t> retInt;
        StatusOr<std::string> retStr;
        switch (propType) {
            case SchemaPropItem::TTL_DURATION:
                retInt = schemaProp->getTtlDuration();
                if (!retInt.ok()) {
                   return retInt.status();
                }
                alterSchemaProp.set_value(folly::to<std::string>(retInt.value()));
                break;
            case SchemaPropItem::TTL_COL:
                // The legality of the column name need be to check in meta
                retStr = schemaProp->getTtlCol();
                if (!retStr.ok()) {
                   return retStr.status();
                }
                alterSchemaProp.set_value(retStr.value());
                break;
            default:
               return Status::Error("Property type not support");
        }
        alterSchemaProp.set_type(std::move(schemaProp->toPropType()));
        schemaProps_.emplace_back(std::move(alterSchemaProp));
    }

    return Status::OK();
}


void AlterTagExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();


    auto future = mc->alterTagSchema(spaceId, *name, std::move(options_), std::move(schemaProps_));
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
