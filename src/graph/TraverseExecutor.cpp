/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "parser/TraverseSentences.h"
#include "graph/GoExecutor.h"
#include "graph/PipeExecutor.h"
#include "graph/OrderByExecutor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

std::unique_ptr<TraverseExecutor> TraverseExecutor::makeTraverseExecutor(Sentence *sentence) {
    return makeTraverseExecutor(sentence, ectx());
}


// static
std::unique_ptr<TraverseExecutor>
TraverseExecutor::makeTraverseExecutor(Sentence *sentence, ExecutionContext *ectx) {
    auto kind = sentence->kind();
    std::unique_ptr<TraverseExecutor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kOrderBy:
            executor = std::make_unique<OrderByExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kUnknown:
            LOG(FATAL) << "Sentence kind unknown";
            break;
        default:
            LOG(FATAL) << "Sentence kind illegal: " << kind;
            break;
    }
    return executor;
}

VariantType Collector::collect(
        const std::string &prop,
        const RowReader *reader,
        RowWriter *writer) {
    DCHECK_NOTNULL(reader);
    VariantType var = getProp(prop, reader);
    switch (var.which()) {
        case 0:
            (*writer) << boost::get<int64_t>(var);
            break;
        case 1:
            (*writer) << boost::get<double>(var);
            break;
        case 2:
            (*writer) << boost::get<bool>(var);
            break;
        case 3:
            (*writer) << boost::get<std::string>(var);
            break;
        default:
            LOG(FATAL) << "Unknown VariantType: " << var.which();
    }

    return var;
}

VariantType Collector::getProp(const std::string &prop,
                               const RowReader *reader) {
    DCHECK_NOTNULL(reader);
    DCHECK_NOTNULL(schema_);
    using nebula::cpp2::SupportedType;
    auto type = schema_->getFieldType(prop).type;
    switch (type) {
        case SupportedType::BOOL: {
            bool v;
            reader->getBool(prop, v);
            return v;
        }
        case SupportedType::INT: {
            int64_t v;
            reader->getInt(prop, v);
            return v;
        }
        case SupportedType::VID: {
            VertexID v;
            reader->getVid(prop, v);
            return v;
        }
        case SupportedType::FLOAT: {
            float v;
            reader->getFloat(prop, v);
            return static_cast<double>(v);
        }
        case SupportedType::DOUBLE: {
            double v;
            reader->getDouble(prop, v);
            return v;
        }
        case SupportedType::STRING: {
            folly::StringPiece v;
            reader->getString(prop, v);
            return v.toString();
        }
        default:
            LOG(FATAL) << "Unknown type: " << static_cast<int32_t>(type);
            return "";
    }
}

}   // namespace graph
}   // namespace nebula
