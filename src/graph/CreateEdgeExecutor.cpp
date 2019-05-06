/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/CreateEdgeExecutor.h"
#include "dataman/ResultSchemaProvider.h"

namespace nebula {
namespace graph {

CreateEdgeExecutor::CreateEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateEdgeSentence*>(sentence);
}


Status CreateEdgeExecutor::prepare() {
    auto status = checkIfGraphSpaceChosen();

    if (!status.ok()) {
        return status;
    }

    const auto& specs = sentence_->columnSpecs();
    auto& schemaOpts = sentence_->getSchemaOpts();

    nebula::cpp2::Schema schema;
    for (auto& spec : specs) {
        nebula::cpp2::ColumnDef column;
        column.name = *spec->name();
        column.type.type = columnTypeToSupportedType(spec->type());
        schema.columns.emplace_back(std::move(column));
    }

    if (schemaOpts.size() != 0) {
        for (auto& schemaOpt : schemaOpts) {
            switch (schemaOpt->getOptType()) {
                case SchemaOptItem::COMMENT:
                    schema.options.set_comment(schemaOpt->getComment());
                    break;
                case SchemaOptItem::ENGINE:
                    schema.options.set_engine(schemaOpt->getEngine());
                    break;
                case SchemaOptItem::ENCRYPT:
                    schema.options.set_encrypt(schemaOpt->getEncrypt());
                    break;
                case SchemaOptItem::COMPRESS:
                    schema.options.set_compress(schemaOpt->getCompress());
                    break;
                case SchemaOptItem::CHARACTER_SET:
                    schema.options.set_character_set(schemaOpt->getCharacterSet());
                    break;
                case SchemaOptItem::COLLATE:
                    schema.options.set_collate(schemaOpt->getCollate());
                    break;
                case SchemaOptItem::TTL_DURATION:
                    schema.options.set_ttl_duration(schemaOpt->getTtlDuration());
                    break;
                case SchemaOptItem::TTL_COl:
                    schema.options.set_ttl_col(schemaOpt->getTtlCol());
                    break;
            }
        }
    }

    return Status::OK();
}


void CreateEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->createEdgeSchema(spaceId, *name, schema);
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
