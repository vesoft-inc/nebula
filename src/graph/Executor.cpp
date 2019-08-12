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
#include "graph/admin/ShowExecutor.h"
#include "graph/admin/AddHostsExecutor.h"
#include "graph/admin/RemoveHostsExecutor.h"
#include "graph/admin/CreateSpaceExecutor.h"
#include "graph/admin/DescribeSpaceExecutor.h"
#include "graph/admin/DropSpaceExecutor.h"
#include "graph/admin/ConfigExecutor.h"
#include "graph/admin/BalanceExecutor.h"
#include "graph/schema/CreateTagExecutor.h"
#include "graph/schema/CreateEdgeExecutor.h"
#include "graph/schema/AlterTagExecutor.h"
#include "graph/schema/AlterEdgeExecutor.h"
#include "graph/schema/DropTagExecutor.h"
#include "graph/schema/DropEdgeExecutor.h"
#include "graph/schema/DescribeTagExecutor.h"
#include "graph/schema/DescribeEdgeExecutor.h"
#include "graph/update/InsertVertexExecutor.h"
#include "graph/update/InsertEdgeExecutor.h"
#include "graph/update/IngestExecutor.h"
#include "graph/query/AssignmentExecutor.h"
#include "graph/query/GoExecutor.h"
#include "graph/query/UseExecutor.h"
#include "graph/query/PipeExecutor.h"
#include "graph/query/OrderByExecutor.h"
#include "graph/query/FetchVerticesExecutor.h"
#include "graph/query/FetchEdgesExecutor.h"
#include "graph/query/YieldExecutor.h"
#include "graph/query/DownloadExecutor.h"
#include "graph/query/SetExecutor.h"
#include "graph/query/FindExecutor.h"
#include "graph/query/MatchExecutor.h"

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
