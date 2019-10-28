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
#include "graph/FetchVerticesExecutor.h"
#include "graph/FetchEdgesExecutor.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/SetExecutor.h"
#include "graph/FindExecutor.h"
#include "graph/MatchExecutor.h"
#include "graph/FindPathExecutor.h"
#include "graph/LimitExecutor.h"

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
        case Sentence::Kind::kFetchVertices:
            executor = std::make_unique<FetchVerticesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFetchEdges:
            executor = std::make_unique<FetchEdgesExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kSet:
            executor = std::make_unique<SetExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kMatch:
            executor = std::make_unique<MatchExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFind:
            executor = std::make_unique<FindExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kFindPath:
            executor = std::make_unique<FindPathExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kLimit:
            executor = std::make_unique<LimitExecutor>(sentence, ectx);
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

void Collector::collect(VariantType &var, RowWriter *writer) const {
    switch (var.which()) {
        case VAR_INT64:
            (*writer) << boost::get<int64_t>(var);
            break;
        case VAR_DOUBLE:
            (*writer) << boost::get<double>(var);
            break;
        case VAR_BOOL:
            (*writer) << boost::get<bool>(var);
            break;
        case VAR_STR:
            (*writer) << boost::get<std::string>(var);
            break;
        default:
            LOG(FATAL) << "Unknown VariantType: " << var.which();
    }
}

VariantType Collector::getProp(const std::string &prop,
                               const RowReader *reader) const {
    DCHECK(reader != nullptr);
    DCHECK(schema_ != nullptr);
    using nebula::cpp2::SupportedType;
    auto type = schema_->getFieldType(prop).type;
    switch (type) {
        case SupportedType::BOOL: {
            bool v;
            reader->getBool(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::TIMESTAMP:
        case SupportedType::INT: {
            int64_t v;
            reader->getInt(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::VID: {
            VertexID v;
            reader->getVid(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::FLOAT: {
            float v;
            reader->getFloat(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return static_cast<double>(v);
        }
        case SupportedType::DOUBLE: {
            double v;
            reader->getDouble(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v;
        }
        case SupportedType::STRING: {
            folly::StringPiece v;
            reader->getString(prop, v);
            VLOG(3) << "get prop: " << prop << ", value: " << v;
            return v.toString();
        }
        default:
            LOG(FATAL) << "Unknown type: " << static_cast<int32_t>(type);
            return "";
    }
}

}   // namespace graph
}   // namespace nebula
