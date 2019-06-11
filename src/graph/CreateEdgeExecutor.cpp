/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    const auto& schemaProps = sentence_->getSchemaProps();

    for (auto& spec : specs) {
        nebula::cpp2::ColumnDef column;
        column.name = *spec->name();
        column.type.type = columnTypeToSupportedType(spec->type());
        schema_.columns.emplace_back(std::move(column));
    }

    if (schemaProps.size() != 0) {
        for (auto& schemaProp : schemaProps) {
            switch (schemaProp->getPropType()) {
                case SchemaPropItem::TTL_DURATION:
                    status = setTTLDuration(schemaProp);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
                case SchemaPropItem::TTL_COL:
                    status = setTTLCol(schemaProp);
                    if (!status.ok()) {
                        return status;
                    }
                    break;
            }
        }
        if (schema_.schema_prop.ttl_duration != 0) {
            // Disable implicit TTL mode
            if (schema_.schema_prop.ttl_col.empty()) {
                return Status::Error("Implicit ttl_col not support");
            }

            // 0 means infinity
            if (schema_.schema_prop.ttl_duration < 0) {
                schema_.schema_prop.set_ttl_duration(0);
            }
        }
    }

     return Status::OK();
}


Status CreateEdgeExecutor::setTTLDuration(SchemaPropItem* schemaProp) {
    auto ret = schemaProp->getTtlDuration();
    if (!ret.ok()) {
        return ret.status();
    }

    auto ttlDuration = ret.value();
    schema_.schema_prop.set_ttl_duration(ttlDuration);
    return Status::OK();
}


Status CreateEdgeExecutor::setTTLCol(SchemaPropItem* schemaProp) {
    auto ret = schemaProp->getTtlCol();
    if (!ret.ok()) {
        return ret.status();
    }

    auto  ttlColName = ret.value();
    // Check the legality of the ttl column name
    for (auto& col : schema_.columns) {
        if (col.name == ttlColName) {
             // Only integer columns and timestamp columns can be used as ttl_col
            if (col.type.type != nebula::cpp2::SupportedType::INT &&
                col.type.type != nebula::cpp2::SupportedType::TIMESTAMP) {
                return Status::Error("Ttl column type illegal");
            }
            schema_.schema_prop.set_ttl_col(ttlColName);
            return Status::OK();
        }
    }
    return Status::Error("Ttl column name not exist in columns");
}


void CreateEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->createEdgeSchema(spaceId, *name, schema_);
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
