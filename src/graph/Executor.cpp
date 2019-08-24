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
        case VAR_INT64:
            writer << boost::get<int64_t>(value);
            break;
        case VAR_DOUBLE:
            writer << boost::get<double>(value);
            break;
        case VAR_BOOL:
            writer << boost::get<bool>(value);
            break;
        case VAR_STR:
            writer << boost::get<std::string>(value);
            break;
        default:
            LOG(FATAL) << "Unknown value type: " << static_cast<uint32_t>(value.which());
    }
}

bool Executor::checkValueType(const nebula::cpp2::ValueType &type, const VariantType &value) {
    switch (value.which()) {
        case VAR_INT64:
            return nebula::cpp2::SupportedType::INT == type.type ||
                   nebula::cpp2::SupportedType::TIMESTAMP == type.type;
        case VAR_DOUBLE:
            return nebula::cpp2::SupportedType::DOUBLE == type.type;
        case VAR_BOOL:
            return nebula::cpp2::SupportedType::BOOL == type.type;
        case VAR_STR:
            return nebula::cpp2::SupportedType::STRING == type.type ||
                   nebula::cpp2::SupportedType::TIMESTAMP == type.type;
        // TODO: Other type
    }

    return false;
}

Status Executor::checkFieldName(std::shared_ptr<const meta::SchemaProviderIf> schema,
                                std::vector<std::string*> props) {
    for (auto fieldIndex = 0u; fieldIndex < schema->getNumFields(); fieldIndex++) {
        auto schemaFieldName = schema->getFieldName(fieldIndex);
        if (UNLIKELY(nullptr == schemaFieldName)) {
            return Status::Error("Invalid field index");
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

StatusOr<int64_t> Executor::toTimestamp(const VariantType &value) {
    if (value.which() != VAR_INT64 && value.which() != VAR_STR) {
        return Status::Error("Invalid value type");
    }

    if (value.which() == VAR_STR) {
        std::string timeStr = boost::get<std::string>(value);
        static const std::regex reg("^[1-9]\\d{3}-"
                                    "(0[1-9]|1[0-2]|\\d)-"
                                    "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                    "(20|21|22|23|[0-1]\\d|\\d):"
                                    "([0-5]\\d|\\d):"
                                    "([0-5]\\d|\\d)$");
        std::smatch result;
        if (!std::regex_search(timeStr, result, reg)) {
            return Status::Error("Invalid timestamp type");
        }
        struct tm time;
        memset(&time, 0, sizeof(time));
        sscanf(timeStr.c_str(), "%d-%d-%d %d:%d:%d",
               &time.tm_year, &time.tm_mon, &time.tm_mday,
               &time.tm_hour, &time.tm_min, &time.tm_sec);

        time.tm_year -= 1900;
        time.tm_mon--;
        auto timestamp = mktime(&time);
        if (timestamp < 0) {
            return Status::Error("Invalid timestamp type");
        }
        return timestamp;
    }
    auto result = boost::get<int64_t>(value);
    if (result < 0) {
        return Status::Error("Invalid timestamp value");
    }
    return result;
}

}   // namespace graph
}   // namespace nebula
