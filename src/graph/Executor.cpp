/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/Executor.h"
#include "parser/TraverseSentences.h"
#include "parser/MutateSentences.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "graph/GoExecutor.h"
#include "graph/UseExecutor.h"
#include "graph/PipeExecutor.h"
#include "graph/CreateTagExecutor.h"
#include "graph/CreateEdgeExecutor.h"
#include "graph/AlterTagExecutor.h"
#include "graph/AlterEdgeExecutor.h"
#include "graph/DropTagExecutor.h"
#include "graph/DropEdgeExecutor.h"
#include "graph/DescribeTagExecutor.h"
#include "graph/DescribeEdgeExecutor.h"
#include "graph/InsertVertexExecutor.h"
#include "graph/InsertEdgeExecutor.h"
#include "graph/AssignmentExecutor.h"
#include "graph/ShowExecutor.h"
#include "graph/AddHostsExecutor.h"
#include "graph/RemoveHostsExecutor.h"
#include "graph/CreateSpaceExecutor.h"
#include "graph/DescribeSpaceExecutor.h"
#include "graph/DropSpaceExecutor.h"
#include "graph/YieldExecutor.h"
#include "graph/DownloadExecutor.h"
#include "graph/OrderByExecutor.h"
#include "graph/IngestExecutor.h"
#include "graph/ConfigExecutor.h"
#include "graph/FetchVerticesExecutor.h"
#include "graph/FetchEdgesExecutor.h"
#include "graph/ConfigExecutor.h"
#include "graph/SetExecutor.h"
#include "graph/FindExecutor.h"
#include "graph/MatchExecutor.h"
#include "graph/BalanceExecutor.h"

namespace nebula {
namespace graph {

std::unique_ptr<Executor> Executor::makeExecutor(Sentence *sentence) {
    auto kind = sentence->kind();
    std::unique_ptr<Executor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUse:
            executor = std::make_unique<UseExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateTag:
            executor = std::make_unique<CreateTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateEdge:
            executor = std::make_unique<CreateEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAlterTag:
            executor = std::make_unique<AlterTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAlterEdge:
            executor = std::make_unique<AlterEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeTag:
            executor = std::make_unique<DescribeTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeEdge:
            executor = std::make_unique<DescribeEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropTag:
             executor = std::make_unique<DropTagExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kDropEdge:
             executor = std::make_unique<DropEdgeExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kInsertVertex:
            executor = std::make_unique<InsertVertexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kInsertEdge:
            executor = std::make_unique<InsertEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kShow:
            executor = std::make_unique<ShowExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAssignment:
            executor = std::make_unique<AssignmentExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAddHosts:
            executor = std::make_unique<AddHostsExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kRemoveHosts:
            executor = std::make_unique<RemoveHostsExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateSpace:
            executor = std::make_unique<CreateSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropSpace:
            executor = std::make_unique<DropSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeSpace:
            executor = std::make_unique<DescribeSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kYield:
            executor = std::make_unique<YieldExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDownload:
            executor = std::make_unique<DownloadExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kOrderBy:
            executor = std::make_unique<OrderByExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kIngest:
            executor = std::make_unique<IngestExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kConfig:
            executor = std::make_unique<ConfigExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFetchVertices:
            executor = std::make_unique<FetchVerticesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFetchEdges:
            executor = std::make_unique<FetchEdgesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kSet:
            executor = std::make_unique<SetExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kMatch:
            executor = std::make_unique<MatchExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFind:
            executor = std::make_unique<FindExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kBalance:
            executor = std::make_unique<BalanceExecutor>(sentence, ectx());
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

std::string Executor::valueTypeToString(nebula::cpp2::ValueType type) {
    switch (type.type) {
        case nebula::cpp2::SupportedType::BOOL:
            return "bool";
        case nebula::cpp2::SupportedType::INT:
            return "int";
        case nebula::cpp2::SupportedType::DOUBLE:
            return "double";
        case nebula::cpp2::SupportedType::STRING:
            return "string";
        case nebula::cpp2::SupportedType::TIMESTAMP:
            return "timestamp";
        default:
            return "unknown";
    }
}

void Executor::writeVariantType(RowWriter &writer, const VariantType &value) {
    switch (value.which()) {
        case 0:
            writer << boost::get<int64_t>(value);
            break;
        case 1:
            writer << boost::get<double>(value);
            break;
        case 2:
            writer << boost::get<bool>(value);
            break;
        case 3:
            writer << boost::get<std::string>(value);
            break;
        default:
            LOG(FATAL) << "Unknown value type: " << static_cast<uint32_t>(value.which());
    }
}

bool Executor::checkValueType(const nebula::cpp2::ValueType &type, const VariantType &value) {
    switch (value.which()) {
        case 0:
            return nebula::cpp2::SupportedType::INT == type.type;
        case 1:
            return nebula::cpp2::SupportedType::DOUBLE == type.type;
        case 2:
            return nebula::cpp2::SupportedType::BOOL == type.type;
        case 3:
            return nebula::cpp2::SupportedType::STRING == type.type;
        // TODO: Other type
    }

    return false;
}

Status Executor::checkFieldName(std::shared_ptr<const meta::SchemaProviderIf> schema,
                                std::vector<std::string*> props) {
    for (auto fieldIndex = 0u; fieldIndex < schema->getNumFields(); fieldIndex++) {
        auto schemaFieldName = schema->getFieldName(fieldIndex);
        if (UNLIKELY(nullptr == schemaFieldName)) {
            return Status::Error("invalid field index");
        }
        if (schemaFieldName != *props[fieldIndex]) {
            LOG(ERROR) << "Field name is wrong, schema field " << schemaFieldName
                       << ", input field " << *props[fieldIndex];
            return Status::Error("Input field name `%s' is wrong",
                                 props[fieldIndex]->c_str());
        }
    }
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
